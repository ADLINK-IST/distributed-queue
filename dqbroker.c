#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <inttypes.h>

#ifndef _WIN32
#include <signal.h>
#include <errno.h>
#else
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#define strdup(x) _strdup(x)
#endif

#include <dds_dcps.h>

#define HOP_RANGE 32
#define ADD_RANGE 64

typedef unsigned (*hh_hash_fn)(const void *);
typedef int (*hh_equals_fn)(const void *, const void *);

struct hh_bucket {
    unsigned hopinfo;
    unsigned inuse;
    char data[];
};

struct hh {
    unsigned size; /* power of 2 */
    size_t elemsz;
    size_t bucketsz;
    char *buckets; /* struct hh_bucket, but embedded data messes up the layout */
    hh_hash_fn hash;
    hh_equals_fn equals;
};

struct hh_iter {
    struct hh *hh;
    unsigned cursor;
};

struct instmap_entry {
    DDS_InstanceHandle_t handle;
    unsigned idx;
};

struct queuesink {
    char *partition;
    DDS_Publisher pub;
    DDS_DataWriter wr;
    unsigned ninst;
};

struct queue {
    struct queue *next; /* very simple admin */
    char *topic_name;
    int is_keyless;
    struct hh *instmap;
    DDS_Topic topic;
    DDS_DataReader source;
    DDS_StatusCondition source_sc;
    unsigned ninst;
    unsigned nsinks;
    unsigned cursor;
    struct queuesink *sinks;
};

struct queueset {
    DDS_WaitSet ws;
    struct queue *queues;
};

struct listener_data {
    struct queueset *qset;
    struct queue *q;
    unsigned i; /* ~0u for reader */
};

static int sigpipe[2];
static DDS_GuardCondition termcond;
#ifdef _WIN32
static HANDLE termev;
#endif
static const char *saved_argv0;
static DDS_DomainParticipant dp;
static DDS_Subscriber data_sub;
static const char *broker_name;
static const char *topic_pattern;
static int keyless_mode_flag = 0;

static void rebalance_new_sink(struct queue *q);
static void rebalance_free_sink(struct queue *q, unsigned idx);

static struct hh_bucket *hh_bucket(const struct hh *hh, unsigned i)
{
    return (struct hh_bucket *) (hh->buckets + i * hh->bucketsz);
}

static struct hh *hh_new(size_t elemsz, unsigned init_size, hh_hash_fn hash, hh_equals_fn equals)
{
    struct hh *hh = malloc(sizeof(*hh));
    unsigned i, size = HOP_RANGE;
    while (size < init_size) {
        size *= 2;
    }
    hh->hash = hash;
    hh->equals = equals;
    hh->size = size;
    hh->elemsz = elemsz;
    hh->bucketsz = sizeof(struct hh_bucket) + ((elemsz+7) & ~(size_t)7);
    hh->buckets = malloc(size * hh->bucketsz);
    for (i = 0; i < size; i++) {
        struct hh_bucket *b = hh_bucket(hh, i);
        b->hopinfo = 0;
        b->inuse = 0;
    }
    return hh;
}

static void hh_free(struct hh *hh)
{
    free(hh->buckets);
    free(hh);
}

static void *hh_lookup_internal(const struct hh *hh, unsigned bucket, const void *template)
{
    const struct hh_bucket *b = hh_bucket(hh, bucket);
    unsigned hopinfo = b->hopinfo;
    if (hopinfo & 1) {
        if (b->inuse && hh->equals(b->data, template)) {
            return (void *) b->data;
        }
    }
    do {
        hopinfo >>= 1;
        if (++bucket == hh->size) {
            bucket = 0;
        }
        if (hopinfo & 1) {
            b = hh_bucket(hh, bucket);
            if (b->inuse && hh->equals(b->data, template)) {
                return (void *) b->data;
            }
        }
    } while (hopinfo != 0);
    return NULL;
}

static void *hh_lookup(const struct hh *hh, const void *template)
{
    const unsigned hash = hh->hash(template);
    const unsigned idxmask = hh->size - 1;
    const unsigned bucket = hash & idxmask;
    return hh_lookup_internal(hh, bucket, template);
}

static unsigned find_closer_free_bucket(struct hh *hh, unsigned free_bucket, unsigned *free_distance)
{
    const unsigned idxmask = hh->size - 1;
    unsigned move_bucket;
    unsigned free_dist;
    move_bucket = (free_bucket - (HOP_RANGE - 1)) & idxmask;
    for (free_dist = HOP_RANGE - 1; free_dist > 0; free_dist--) {
        struct hh_bucket * const mb = hh_bucket(hh, move_bucket);
        unsigned mask = 1;
        unsigned move_free_dist;
        for (move_free_dist = 0; move_free_dist < free_dist; move_free_dist++, mask <<= 1) {
            if (mask & mb->hopinfo) {
                break;
            }
        }
        if (move_free_dist < free_dist) {
            unsigned new_free_bucket = (move_bucket + move_free_dist) & idxmask;
            struct hh_bucket * const fb = hh_bucket(hh, free_bucket);
            struct hh_bucket * const nfb = hh_bucket(hh, new_free_bucket);
            mb->hopinfo |= 1u << free_dist;
            fb->inuse = 1;
            memcpy(fb->data, nfb->data, hh->elemsz);
            fb->inuse = 0;
            mb->hopinfo &= ~(1u << move_free_dist);
            *free_distance -= free_dist - move_free_dist;
            return new_free_bucket;
        }
        move_bucket =(move_bucket + 1) & idxmask;
    }
    return ~0u;
}

static void hh_resize(struct hh *hh)
{
    unsigned i, idxmask0, idxmask1;
    char *bs1 = malloc(2 * hh->size * hh->bucketsz);
    struct hh hh1;
    hh1.buckets = bs1;
    hh1.bucketsz = hh->bucketsz;
    for (i = 0; i < 2 * hh->size; i++) {
        struct hh_bucket *b = hh_bucket(&hh1, i);
        b->hopinfo = 0;
        b->inuse = 0;
    }
    idxmask0 = hh->size - 1;
    idxmask1 = 2 * hh->size - 1;
    for (i = 0; i < hh->size; i++) {
        struct hh_bucket const * const b = hh_bucket(hh, i);
        if (b->inuse) {
            const unsigned hash = hh->hash(b->data);
            const unsigned old_start_bucket = hash & idxmask0;
            const unsigned new_start_bucket = hash & idxmask1;
            const unsigned dist = (i >= old_start_bucket) ? (i - old_start_bucket) : (hh->size + i - old_start_bucket);
            const unsigned newb = (new_start_bucket + dist) & idxmask1;
            struct hh_bucket * const nsb = hh_bucket(&hh1, new_start_bucket);
            struct hh_bucket * const nb = hh_bucket(&hh1, newb);
            assert(0 <= dist && dist < HOP_RANGE);
            assert(!nb->inuse);
            nsb->hopinfo |= 1u << dist;
            nb->inuse = 1;
            memcpy(nb->data, b->data, hh->elemsz);
        }
    }
    free(hh->buckets);
    hh->size *= 2;
    hh->buckets = bs1;
}

static int hh_add(struct hh *hh, const void *data)
{
    const unsigned hash = hh->hash(data);
    const unsigned idxmask = hh->size - 1;
    const unsigned start_bucket = hash & idxmask;
    unsigned free_distance, free_bucket;
    if (hh_lookup_internal(hh, start_bucket, data)) {
        return 0;
    }
    free_bucket = start_bucket;
    for (free_distance = 0; free_distance < ADD_RANGE; free_distance++) {
        struct hh_bucket const * const fb = hh_bucket(hh, free_bucket);
        if (!fb->inuse) {
            break;
        }
        free_bucket = (free_bucket + 1) & idxmask;
    }
    if (free_distance < ADD_RANGE) {
        do {
            if (free_distance < HOP_RANGE) {
                struct hh_bucket * const sb = hh_bucket(hh, start_bucket);
                struct hh_bucket * const fb = hh_bucket(hh, free_bucket);
                assert((unsigned) free_bucket == ((start_bucket + free_distance) & idxmask));
                sb->hopinfo |= 1u << free_distance;
                fb->inuse = 1;
                memcpy(fb->data, data, hh->elemsz);
                assert(hh_lookup_internal(hh, start_bucket, data));
                return 1;
            }
            free_bucket = find_closer_free_bucket(hh, free_bucket, &free_distance);
            assert(free_bucket == ~0u || free_bucket <= idxmask);
        } while (free_bucket <= idxmask);
    }
    hh_resize(hh);
    return hh_add(hh, data);
}

static void *hh_iter_next(struct hh_iter *iter)
{
    struct hh *rt = iter->hh;
    while (iter->cursor < rt->size) {
        struct hh_bucket *b = hh_bucket(rt, iter->cursor);
        iter->cursor++;
        if (b->inuse) {
            return b->data;
        }
    }
    return NULL;
}

static void *hh_iter_first(struct hh *hh, struct hh_iter *iter)
{
    iter->hh = hh;
    iter->cursor = 0;
    return hh_iter_next(iter);
}

void error (const char *fmt, ...)
{
    va_list ap;
    fprintf (stderr, "%s: error: ", saved_argv0);
    va_start (ap, fmt);
    vfprintf (stderr, fmt, ap);
    va_end (ap);
    exit (2);
}

const char *dds_strerror (DDS_ReturnCode_t code)
{
    switch (code) {
        case DDS_RETCODE_OK: return "ok";
        case DDS_RETCODE_ERROR: return "error";
        case DDS_RETCODE_UNSUPPORTED: return "unsupported";
        case DDS_RETCODE_BAD_PARAMETER: return "bad parameter";
        case DDS_RETCODE_PRECONDITION_NOT_MET: return "precondition not met";
        case DDS_RETCODE_OUT_OF_RESOURCES: return "out of resources";
        case DDS_RETCODE_NOT_ENABLED: return "not enabled";
        case DDS_RETCODE_IMMUTABLE_POLICY: return "immutable policy";
        case DDS_RETCODE_INCONSISTENT_POLICY: return "inconsistent policy";
        case DDS_RETCODE_ALREADY_DELETED: return "already deleted";
        case DDS_RETCODE_TIMEOUT: return "timeout";
        case DDS_RETCODE_NO_DATA: return "no data";
        case DDS_RETCODE_ILLEGAL_OPERATION: return "illegal operation";
        default: return "(undef)";
    }
}

#ifndef _WIN32
static void sigh (int sig)
{
    const char c = 1;
    ssize_t r;
    (void)sig;
    do {
        r = write (sigpipe[1], &c, 1);
    } while (r == -1 && errno == EINTR);
}

static void *sigthread(void *varg)
{
    (void)varg;
    while (1) {
        char c;
        ssize_t r;
        if ((r = read (sigpipe[0], &c, 1)) < 0) {
            if (errno == EINTR) {
                continue;
            }
            error ("sigthread: read failed, errno %d\n", (int) errno);
        } else if (r == 0) {
            error ("sigthread: unexpected eof\n");
        } else if (c == 0) {
            break;
        } else {
            DDS_GuardCondition_set_trigger_value(termcond, 1);
        }
    }
    return NULL;
}
#else
static BOOL WINAPI console_ctrl_handler(DWORD ctrl_type)
{
    if (ctrl_type == CTRL_LOGOFF_EVENT) {
        return FALSE;
    } else {
        /* Windows kills the process after a few seconds, perhaps less than 10,
           but it doesn't really matter - it gets killed either way */
        DDS_GuardCondition_set_trigger_value(termcond, 1);
        WaitForSingleObject(termev, 10000);
        return TRUE;
    }
}
#endif

static void usage (void)
{
    fprintf (stderr, "%s [-1] NAME TOPIC-PATTERN\n\
\n\
-1     treat all topics as keyless (having ony 1 instance)\n\
", saved_argv0);
    exit (1);
}

static char *get_topic_keylist(DDS_Topic tp)
{
    /* Don't quite know when the thing is keyless, but this test seems to work */
    char *kl = DDS_Topic_get_keylist(tp);
    if (kl == NULL) {
        return NULL;
    } else if (strcmp(kl, "") == 0 || strcmp(kl, "NULL") == 0) {
        DDS_free(kl);
        return NULL;
    } else {
        return kl;
    }
}

static unsigned hash_instance_handle(const DDS_InstanceHandle_t h)
{
#define C(x,y,z) (((DDS_unsigned_long_long)(x) * 1000000u + (y)) * 1000000u + (z))
    const DDS_unsigned_long_long hc0 = C(16292676, 669999, 574021);
    const DDS_unsigned_long_long hc1 = C(10242350, 189706, 880077);
#undef C
    const DDS_unsigned_long a = (DDS_unsigned_long)(h >> 32);
    const DDS_unsigned_long b = (DDS_unsigned_long)h;
    return (unsigned) (((a + hc0) * (b + hc1)) >> 32);
}

static unsigned instmap_hash(const void *vx)
{
    const struct instmap_entry *x = vx;
    return hash_instance_handle(x->handle);
}

static int instmap_equals(const void *va, const void *vb)
{
    const struct instmap_entry *a = va;
    const struct instmap_entry *b = vb;
    return a->handle == b->handle;
}

static void register_type_support(DDS_Topic tp)
{
    char *tn = DDS_Topic_get_type_name(tp);
    char *kl = get_topic_keylist(tp);
    char *md = DDS_Topic_get_metadescription(tp);
    DDS_ReturnCode_t rc;
    DDS_TypeSupport ts;
    if ((ts = DDS_TypeSupport__alloc(tn, kl ? kl : "", md)) == NULL)
        error("DDS_TypeSupport__alloc(%s) failed\n", tn);
    if ((rc = DDS_TypeSupport_register_type(ts, dp, tn)) != DDS_RETCODE_OK)
        error("DDS_TypeSupport_register_type(%s) failed (%s)\n", tn, dds_strerror(rc));
    DDS_free(md);
    if (kl) { DDS_free(kl); }
    DDS_free(tn);
    DDS_free(ts);
}

static int topic_is_keyless(DDS_Topic tp)
{
    if (keyless_mode_flag) {
        return 1;
    } else {
        char *kl = get_topic_keylist(tp);
        if (kl == NULL) {
            return 1;
        } else {
            DDS_free(kl);
            return 0;
        }
    }
}

static struct queue *new_queue(struct queueset *qset, const char *topic_name)
{
    DDS_Duration_t timeout = DDS_DURATION_INFINITE;
    DDS_ReturnCode_t rc;
    DDS_Topic tp;
    struct queue *q;
    printf("new queue %s\n", topic_name);
    /* First call to find_topic will succeed, but without a type support. The topic
       allows us to get the information needed to register a generic type support, and
       after that we can register the topic properly. Without this strange detour,
       it appears to work, but there'll be a double-free when shutting down.
     
       'Tis but an undocumented interface! */
    if ((tp = DDS_DomainParticipant_find_topic(dp, topic_name, &timeout)) == NULL) {
        error("handle_create_writer: DDS_DomainParticipant_find_topic(1) failed\n");
    }
    register_type_support(tp);
    if ((rc = DDS_DomainParticipant_delete_topic(dp, tp)) != DDS_RETCODE_OK) {
        error("handle_create_writer: DDS_DomainParticipant_find_topic failed (%s)\n", dds_strerror(rc));
    }
    if ((tp = DDS_DomainParticipant_find_topic(dp, topic_name, &timeout)) == NULL) {
        error("handle_create_writer: DDS_DomainParticipant_find_topic(2) failed\n");
    }
    q = malloc(sizeof(*q));
    q->topic_name = strdup(topic_name);
    q->topic = tp;
    q->is_keyless = topic_is_keyless(tp);
    if (q->is_keyless) {
        q->instmap = NULL;
    } else {
        q->instmap = hh_new(sizeof(struct instmap_entry), 32, instmap_hash, instmap_equals);
    }
    q->source = NULL;
    q->source_sc = NULL;
    q->ninst = 0;
    q->nsinks = 0;
    q->cursor = 0;
    q->sinks = malloc(0);
    q->next = qset->queues;
    qset->queues = q;
    return q;
}

static void maybe_free_queue (struct queueset *qset, struct queue *q)
{
    if (q->source == NULL && q->nsinks == 0) {
        DDS_ReturnCode_t rc;
        struct queue *t = qset->queues, **pt = &qset->queues;
        while (t != NULL && t != q) {
            pt = &t->next;
            t = t->next;
        }
        assert(t != NULL);
        *pt = q->next;
        printf("free queue %s\n", q->topic_name);
        if ((rc = DDS_DomainParticipant_delete_topic(dp, q->topic)) != DDS_RETCODE_OK) {
            error("handle_create_writer: DDS_DomainParticipant_delete_topic failed (%s)\n", dds_strerror(rc));
        }
        if (q->instmap) {
            hh_free(q->instmap);
        }
        free(q->topic_name);
        free(q->sinks);
        free(q);
    }
}

static void free_queue_source(struct queueset *qset, struct queue *q)
{
    DDS_ReturnCode_t rc;
    DDS_StatusCondition sc = DDS_Entity_get_statuscondition(q->source);
    printf ("writer %s gone, deleting reader\n", q->topic_name);
    if ((rc = DDS_WaitSet_detach_condition(qset->ws, sc)) != DDS_RETCODE_OK) {
        error("free_queue_source: DDS_WaitSet_detach_condition failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_Subscriber_delete_datareader(data_sub, q->source)) != DDS_RETCODE_OK) {
        error("free_queue_source: DDS_Subscriber_delete_datareader failed (%s)\n", dds_strerror(rc));
    }
    q->source = NULL;
    q->source_sc = NULL;
}

static void free_queue_sink(struct queue *q, unsigned i)
{
    DDS_ReturnCode_t rc;
    printf ("reader %s.%s gone, deleting writer\n", q->topic_name, q->sinks[i].partition);
    if ((rc = DDS_Publisher_delete_datawriter(q->sinks[i].pub, q->sinks[i].wr)) != DDS_RETCODE_OK) {
        error("free_queue_sink: DDS_Publisher_delete_datawriter failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_DomainParticipant_delete_publisher(dp, q->sinks[i].pub)) != DDS_RETCODE_OK) {
        error("free_queue_sink: DDS_DomainParticipant_delete_publisher failed (%s)\n", dds_strerror(rc));
    }
    free(q->sinks[i].partition);
    q->nsinks--;
    if (q->nsinks > 0 && i != q->nsinks) {
        assert(i < q->nsinks);
        q->sinks[i] = q->sinks[q->nsinks];
    }
    if (q->cursor == q->nsinks) {
        q->cursor = 0;
    }
    assert (q->nsinks == 0 || q->cursor < q->nsinks);

    if (q->instmap) {
        rebalance_free_sink(q, i);
    }
}

static void instancehandle_to_id (uint32_t *systemId, uint32_t *localId, DDS_InstanceHandle_t h)
{
#if BYTE_ORDER == BIG_ENDIAN
    *systemId = (unsigned) (h >> 32) & ~0x80000000;
    *localId = (unsigned) h;
#else
    *systemId = (unsigned) h & ~0x80000000;
    *localId = (unsigned) (h >> 32);
#endif
}

static void rd_on_liveliness_changed (void *listener_data, DDS_DataReader rd, const DDS_LivelinessChangedStatus *status)
{
    struct listener_data *ld = listener_data;
    uint32_t systemId, localId;
    assert(ld->i == ~0u);
    assert(ld->q->source == NULL || ld->q->source == rd);
    instancehandle_to_id(&systemId, &localId, status->last_publication_handle);
    printf ("[liveliness-changed(%s): alive=(%d change %d) not_alive=(%d change %d) handle=%" PRIx32 ":%" PRIx32 "]\n",
            ld->q->topic_name,
            status->alive_count, status->alive_count_change,
            status->not_alive_count, status->not_alive_count_change,
            systemId, localId);
}

static void rd_on_sample_lost (void *listener_data, DDS_DataReader rd, const DDS_SampleLostStatus *status)
{
    struct listener_data *ld = listener_data;
    assert(ld->i == ~0u);
    assert(ld->q->source == NULL || ld->q->source == rd);
    printf ("[sample-lost(%s): total=(%d change %d)]\n",
            ld->q->topic_name, status->total_count, status->total_count_change);
}

static void rd_on_sample_rejected (void *listener_data, DDS_DataReader rd, const DDS_SampleRejectedStatus *status)
{
    struct listener_data *ld = listener_data;
    const char *reasonstr = "?";
    assert(ld->i == ~0u);
    assert(ld->q->source == NULL || ld->q->source == rd);
    switch (status->last_reason) {
        case DDS_NOT_REJECTED: reasonstr = "not_rejected"; break;
        case DDS_REJECTED_BY_INSTANCES_LIMIT: reasonstr = "instances"; break;
        case DDS_REJECTED_BY_SAMPLES_LIMIT: reasonstr = "samples"; break;
        case DDS_REJECTED_BY_SAMPLES_PER_INSTANCE_LIMIT: reasonstr = "samples_per_instance"; break;
    }
    printf("[sample-rejected(%s): total=(%d change %d) reason=%s handle=%lld]\n",
           ld->q->topic_name,
           status->total_count, status->total_count_change,
           reasonstr,
           status->last_instance_handle);
}

static void rd_on_subscription_matched(void *listener_data, DDS_DataReader rd, const DDS_SubscriptionMatchedStatus *status)
{
    struct listener_data *ld = listener_data;
    uint32_t systemId, localId;
    assert(ld->i == ~0u);
    assert(ld->q->source == NULL || ld->q->source == rd);
    instancehandle_to_id(&systemId, &localId, status->last_publication_handle);
    printf("[subscription-matched(%s): total=(%d change %d) current=(%d change %d) handle=%" PRIx32 ":%" PRIx32 "]\n",
           ld->q->topic_name,
           status->total_count, status->total_count_change,
           status->current_count, status->current_count_change,
           systemId, localId);
}

static void rd_on_requested_deadline_missed(void *listener_data, DDS_DataReader rd, const DDS_RequestedDeadlineMissedStatus *status)
{
    struct listener_data *ld = listener_data;
    uint32_t systemId, localId;
    assert(ld->i == ~0u);
    assert(ld->q->source == NULL || ld->q->source == rd);
    instancehandle_to_id(&systemId, &localId, status->last_instance_handle);
    printf("[requested-deadline-missed(%s): total=(%d change %d) handle=%" PRIx32 ":%" PRIx32 "]\n",
           ld->q->topic_name,
           status->total_count, status->total_count_change,
           systemId, localId);
}

static const char *policystr(DDS_QosPolicyId_t id)
{
    switch(id) {
        case DDS_USERDATA_QOS_POLICY_ID: return DDS_USERDATA_QOS_POLICY_NAME;
        case DDS_DURABILITY_QOS_POLICY_ID: return DDS_DURABILITY_QOS_POLICY_NAME;
        case DDS_PRESENTATION_QOS_POLICY_ID: return DDS_PRESENTATION_QOS_POLICY_NAME;
        case DDS_DEADLINE_QOS_POLICY_ID: return DDS_DEADLINE_QOS_POLICY_NAME;
        case DDS_LATENCYBUDGET_QOS_POLICY_ID: return DDS_LATENCYBUDGET_QOS_POLICY_NAME;
        case DDS_OWNERSHIP_QOS_POLICY_ID: return DDS_OWNERSHIP_QOS_POLICY_NAME;
        case DDS_OWNERSHIPSTRENGTH_QOS_POLICY_ID: return DDS_OWNERSHIPSTRENGTH_QOS_POLICY_NAME;
        case DDS_LIVELINESS_QOS_POLICY_ID: return DDS_LIVELINESS_QOS_POLICY_NAME;
        case DDS_TIMEBASEDFILTER_QOS_POLICY_ID: return DDS_TIMEBASEDFILTER_QOS_POLICY_NAME;
        case DDS_PARTITION_QOS_POLICY_ID: return DDS_PARTITION_QOS_POLICY_NAME;
        case DDS_RELIABILITY_QOS_POLICY_ID: return DDS_RELIABILITY_QOS_POLICY_NAME;
        case DDS_DESTINATIONORDER_QOS_POLICY_ID: return DDS_DESTINATIONORDER_QOS_POLICY_NAME;
        case DDS_HISTORY_QOS_POLICY_ID: return DDS_HISTORY_QOS_POLICY_NAME;
        case DDS_RESOURCELIMITS_QOS_POLICY_ID: return DDS_RESOURCELIMITS_QOS_POLICY_NAME;
        case DDS_ENTITYFACTORY_QOS_POLICY_ID: return DDS_ENTITYFACTORY_QOS_POLICY_NAME;
        case DDS_WRITERDATALIFECYCLE_QOS_POLICY_ID: return DDS_WRITERDATALIFECYCLE_QOS_POLICY_NAME;
        case DDS_READERDATALIFECYCLE_QOS_POLICY_ID: return DDS_READERDATALIFECYCLE_QOS_POLICY_NAME;
        case DDS_TOPICDATA_QOS_POLICY_ID: return DDS_TOPICDATA_QOS_POLICY_NAME;
        case DDS_GROUPDATA_QOS_POLICY_ID: return DDS_GROUPDATA_QOS_POLICY_NAME;
        case DDS_TRANSPORTPRIORITY_QOS_POLICY_ID: return DDS_TRANSPORTPRIORITY_QOS_POLICY_NAME;
        case DDS_LIFESPAN_QOS_POLICY_ID: return DDS_LIFESPAN_QOS_POLICY_NAME;
        case DDS_DURABILITYSERVICE_QOS_POLICY_ID: return DDS_DURABILITYSERVICE_QOS_POLICY_NAME;
        case DDS_SUBSCRIPTIONKEY_QOS_POLICY_ID: return DDS_SUBSCRIPTIONKEY_QOS_POLICY_NAME;
        case DDS_VIEWKEY_QOS_POLICY_ID: return DDS_VIEWKEY_QOS_POLICY_NAME;
        case DDS_READERLIFESPAN_QOS_POLICY_ID: return DDS_READERLIFESPAN_QOS_POLICY_NAME;
        case DDS_SHARE_QOS_POLICY_ID: return DDS_SHARE_QOS_POLICY_NAME;
        case DDS_SCHEDULING_QOS_POLICY_ID: return DDS_SCHEDULING_QOS_POLICY_NAME;
        default: return "?";
    }
}

static void format_policies(char *polstr, size_t polsz, const DDS_QosPolicyCount *xs, unsigned nxs)
{
    char *ps = polstr;
    unsigned i;
    for(i = 0; i < nxs && ps < polstr + polsz; i++) {
        const DDS_QosPolicyCount *x = &xs[i];
        int n = snprintf(ps, polstr + polsz - ps, "%s%s:%d", i == 0 ? "" : ", ", policystr(x->policy_id), x->count);
        ps += n;
    }
}

static void rd_on_requested_incompatible_qos(void *listener_data, DDS_DataReader rd, const DDS_RequestedIncompatibleQosStatus *status)
{
    struct listener_data *ld = listener_data;
    char polstr[1024] = "";
    assert(ld->i == ~0u);
    assert(ld->q->source == NULL || ld->q->source == rd);
    format_policies(polstr, sizeof(polstr), status->policies._buffer, status->policies._length);
    printf("[requested-incompatible-qos(%s): total=(%d change %d) last_policy=%s {%s}]\n",
           ld->q->topic_name,
           status->total_count, status->total_count_change,
           policystr(status->last_policy_id), polstr);
}

static void wr_on_offered_incompatible_qos(void *listener_data, DDS_DataWriter wr, const DDS_OfferedIncompatibleQosStatus *status)
{
    struct listener_data *ld = listener_data;
    char polstr[1024] = "";
    assert(ld->i < ld->q->nsinks);
    assert(ld->q->sinks[ld->i].wr == wr);
    format_policies(polstr, sizeof(polstr), status->policies._buffer, status->policies._length);
    printf("[offered-incompatible-qos(%s.%s): total=(%d change %d) last_policy=%s {%s}]\n",
           ld->q->topic_name, ld->q->sinks[ld->i].partition,
           status->total_count, status->total_count_change,
           policystr(status->last_policy_id), polstr);
}

static void wr_on_liveliness_lost(void *listener_data, DDS_DataWriter wr, const DDS_LivelinessLostStatus *status)
{
    struct listener_data *ld = listener_data;
    assert(ld->i < ld->q->nsinks);
    assert(ld->q->sinks[ld->i].wr == wr);
    printf("[liveliness-lost(%s.%s): total=(%d change %d)]\n",
           ld->q->topic_name, ld->q->sinks[ld->i].partition,
           status->total_count, status->total_count_change);
}

static void wr_on_offered_deadline_missed(void *listener_data, DDS_DataWriter wr, const DDS_OfferedDeadlineMissedStatus *status)
{
    struct listener_data *ld = listener_data;
    assert(ld->i < ld->q->nsinks);
    assert(ld->q->sinks[ld->i].wr == wr);
    printf("[offered-deadline-missed(%s.%s): total=(%d change %d) handle=%lld]\n",
           ld->q->topic_name, ld->q->sinks[ld->i].partition,
           status->total_count, status->total_count_change, status->last_instance_handle);
}

static void wr_on_publication_matched(void *listener_data, DDS_DataWriter wr, const DDS_PublicationMatchedStatus *status)
{
    struct listener_data *ld = listener_data;
    uint32_t systemId, localId;
    assert(ld->i < ld->q->nsinks);
    assert(ld->q->sinks[ld->i].wr == wr);
    instancehandle_to_id(&systemId, &localId, status->last_subscription_handle);
    printf("[publication-matched(%s.%s): total=(%d change %d) current=(%d change %d) handle=%" PRIx32 ":%" PRIx32 "]\n",
           ld->q->topic_name, ld->q->sinks[ld->i].partition,
           status->total_count, status->total_count_change,
           status->current_count, status->current_count_change,
           systemId, localId);
}

static int patmatch(const char *pat, const char *str)
{
    switch(*pat) {
        case 0:   return *str == 0;
        case '?': return *str == 0 ? 0 : patmatch(pat+1, str+1);
        case '*': return patmatch(pat+1, str) || (*str != 0 && patmatch(pat, str+1));
        default:  return *str != *pat ? 0 : patmatch(pat+1, str+1);
    }
}

static DDS_StatusCondition attach_reader_status_data_available(DDS_WaitSet ws, DDS_DataReader rd)
{
    DDS_StatusCondition sc;
    DDS_ReturnCode_t rc;
    if ((sc = DDS_DataReader_get_statuscondition(rd)) == NULL) {
        error("DDS_DataReader_get_statuscondition failed\n");
    }
    if ((rc = DDS_StatusCondition_set_enabled_statuses(sc, DDS_DATA_AVAILABLE_STATUS)) != DDS_RETCODE_OK) {
        error("DDS_StatusCondition_set_enabled_statuses failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_WaitSet_attach_condition(ws, sc)) != DDS_RETCODE_OK) {
        error("DDS_WaitSet_attach_condition failed (%s)\n", dds_strerror(rc));
    }
    return sc;
}

static int have_known_matching_writers(DDS_PublicationBuiltinTopicDataDataReader rd, const char *topic_name, const char *partition)
{
    /* It is very complicated to use the subscription_matched information because it
     gets updated asynchronously (this also a problem when using the corresponding
     listeners). The READ / NOT_READ flag gets updated synchronously, so that we
     can use to only consider already known matching endpoints. We also have to
     ignore existing endpoints that we haven't "discovered" yet, because those may
     already have disappeared by the time we would get around to discovering them.
     Sadly, queries can't look inside sequences, so we have to scan them ...

     Alternatively (and obviously seemingly simpler) would be to maintain a reference
     count in the queue, but there is a small risk of the writer disappearing and
     re-appearing (which would mean a transition through DISPOSED, and the view state
     becoming NEW again, and thus trigger processing corresponding to a new writer)
     without us getting around to deleting the writer in between. This can be detected
     by looking at the disposed_generation_count, but that in turn requires tracking
     not only how many writers exist, but also which writers.

     In C++, C#, Java, Scala, Haskell, &c., you wouldn't mind - just add a map of known
     writers and go from there. This being C (and it has to C to use this undocumented
     generic API) that means a lot of typing ... so I might as well exercise the DCPS
     API a bit more. */
    char *qpbuf[1];
    DDS_StringSeq qp = { 1, 1, qpbuf, 0 };
    DDS_QueryCondition q;
    DDS_SampleInfoSeq iseq;
    DDS_sequence_DDS_PublicationBuiltinTopicData mseq;
    DDS_ReturnCode_t rc;
    DDS_unsigned_long i, j;
    int found = 0;

    qpbuf[0] = (char *) topic_name;
    if ((q = DDS_DataReader_create_querycondition(rd, DDS_READ_SAMPLE_STATE, DDS_ANY_VIEW_STATE, DDS_ALIVE_INSTANCE_STATE, "topic_name = %0", &qp)) == NULL) {
        error("have_known_matching_writers: DDS_DataReader_create_querycondition failed\n");
    }

    memset(&iseq, 0, sizeof(iseq));
    memset(&mseq, 0, sizeof(mseq));
    if ((rc = DDS_PublicationBuiltinTopicDataDataReader_read_w_condition(rd, &mseq, &iseq, DDS_LENGTH_UNLIMITED, q)) == DDS_RETCODE_OK) {
        for (i = 0; i < iseq._length && !found; i++) {
            if (iseq._buffer[i].valid_data) {
                DDS_PublicationBuiltinTopicData *d = &mseq._buffer[i];
                for (j = 0; j < d->partition.name._length; j++) {
                    if (strcmp(d->partition.name._buffer[j], partition) == 0) {
                        found = 1;
                        break;
                    }
                }
            }
        }
    } else if (rc != DDS_RETCODE_NO_DATA) {
        error("have_known_matching_writers: DDS_PublicationBuiltinTopicDataDataReader_take failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_PublicationBuiltinTopicDataDataReader_return_loan(rd, &mseq, &iseq)) != DDS_RETCODE_OK) {
        error("have_known_matching_writers: DDS_PublicationBuiltinTopicDataDataReader_return_loan(1) failed (%s)\n", dds_strerror(rc));
    }

    if ((rc = DDS_DataReader_delete_readcondition(rd, q)) != DDS_RETCODE_OK) {
        error("have_known_matching_writers: DDS_DataReader_delete_readcondition failed (%s)\n", dds_strerror(rc));
    }
    return found;
}

static int have_known_matching_readers(DDS_SubscriptionBuiltinTopicDataDataReader rd, const char *topic_name, const char *partition)
{
    char *qpbuf[1];
    DDS_StringSeq qp;
    DDS_QueryCondition q;
    DDS_SampleInfoSeq iseq;
    DDS_sequence_DDS_SubscriptionBuiltinTopicData mseq;
    DDS_ReturnCode_t rc;
    DDS_unsigned_long i;
    int found = 0;

    qp._length = qp._maximum = 1;
    qp._buffer = qpbuf;
    qpbuf[0] = (char *) topic_name;
    if ((q = DDS_DataReader_create_querycondition(rd, DDS_READ_SAMPLE_STATE, DDS_ANY_VIEW_STATE, DDS_ALIVE_INSTANCE_STATE, "topic_name = %0", &qp)) == NULL) {
        error("have_known_matching_readers: DDS_DataReader_create_querycondition failed\n");
    }

    memset(&iseq, 0, sizeof(iseq));
    memset(&mseq, 0, sizeof(mseq));
    if ((rc = DDS_SubscriptionBuiltinTopicDataDataReader_read_w_condition(rd, &mseq, &iseq, DDS_LENGTH_UNLIMITED, q)) == DDS_RETCODE_OK) {
        for (i = 0; i < iseq._length && !found; i++) {
            if (iseq._buffer[i].valid_data) {
                DDS_SubscriptionBuiltinTopicData *d = &mseq._buffer[i];
                if (d->partition.name._length == 1 && strcmp(d->partition.name._buffer[0], partition) == 0) {
                    found = 1;
                }
            }
        }
    } else if (rc != DDS_RETCODE_NO_DATA) {
        error("have_known_matching_readers: DDS_PublicationBuiltinTopicDataDataReader_take failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_SubscriptionBuiltinTopicDataDataReader_return_loan(rd, &mseq, &iseq)) != DDS_RETCODE_OK) {
        error("have_known_matching_readers: DDS_PublicationBuiltinTopicDataDataReader_return_loan(1) failed (%s)\n", dds_strerror(rc));
    }

    if ((rc = DDS_DataReader_delete_readcondition(rd, q)) != DDS_RETCODE_OK) {
        error("have_known_matching_readers: DDS_DataReader_delete_readcondition failed (%s)\n", dds_strerror(rc));
    }
    return found;
}

static struct listener_data *new_listener_data(struct queueset *qset, struct queue *q, unsigned i)
{
    struct listener_data *ld = malloc(sizeof(*ld));
    ld->qset = qset;
    ld->q = q;
    ld->i = i;
    return ld;
}

static int accept_writer_as_queue_source(const DDS_PublicationBuiltinTopicData *wrdesc)
{
    DDS_unsigned_long i;
    if (patmatch(topic_pattern, wrdesc->topic_name)) {
        for (i = 0; i < wrdesc->partition.name._length; i++) {
            if (strcmp(wrdesc->partition.name._buffer[i], broker_name) == 0) {
                return 1;
            }
        }
    }
    return 0;
}

static struct queue *lookup_queue(struct queueset *qset, const char *topic_name)
{
    struct queue *q;
    for (q = qset->queues; q != NULL; q = q->next) {
        if (strcmp(q->topic_name, topic_name) == 0) {
            return q;
        }
    }
    return NULL;
}

static void handle_create_writer(struct queueset *qset, const DDS_PublicationBuiltinTopicData *wrdesc)
{
    DDS_DataReaderQos *rqos;
    DDS_ReturnCode_t rc;
    DDS_TopicQos *tqos;
    struct DDS_DataReaderListener rdlistener;
    DDS_StatusMask rdmask;
    struct queue *q;

    /* No need to do anything for non-matching writers and for writers for known queues */
    if (!accept_writer_as_queue_source(wrdesc)) {
        return;
    }
    if ((q = lookup_queue(qset, wrdesc->topic_name)) != NULL && q->source != NULL) {
        return;
    }

    if (q == NULL) {
        q = new_queue(qset, wrdesc->topic_name);
    }

    /* Create a matching data reader (we can just reuse the same publisher) */
    if ((tqos = DDS_TopicQos__alloc()) == NULL) {
        error("handle_create_writer: DDS_TopicQos__alloc failed\n");
    }
    if ((rc = DDS_Topic_get_qos(q->topic, tqos)) != DDS_RETCODE_OK) {
        error("handle_create_writer: DDS_Topic_get_qos failed (%s)\n", dds_strerror(rc));
    }
    if ((rqos = DDS_DataReaderQos__alloc()) == NULL) {
        error("handle_create_writer: DDS_DataReaderQos__alloc failed\n");
    }
    if ((rc = DDS_Subscriber_get_default_datareader_qos(data_sub, rqos)) != DDS_RETCODE_OK) {
        error("handle_create_writer: DDS_Subscriber_get_default_datareader_qos failed (%s)\n", dds_strerror(rc));
    }
    rqos->durability = wrdesc->durability;
    rqos->latency_budget.duration.sec = DDS_DURATION_INFINITE_SEC;
    rqos->latency_budget.duration.nanosec = DDS_DURATION_INFINITE_NSEC;
    rqos->reliability = wrdesc->reliability;
    rqos->destination_order.kind = DDS_BY_RECEPTION_TIMESTAMP_DESTINATIONORDER_QOS;
    rqos->history.kind = DDS_KEEP_ALL_HISTORY_QOS;
    rqos->resource_limits.max_samples = 10;
    rqos->ownership = wrdesc->ownership;

    memset (&rdlistener, 0, sizeof (rdlistener));
    rdlistener.listener_data = new_listener_data(qset, q, ~0u);
    rdlistener.on_liveliness_changed = rd_on_liveliness_changed;
    rdlistener.on_sample_lost = rd_on_sample_lost;
    rdlistener.on_sample_rejected = rd_on_sample_rejected;
    rdlistener.on_subscription_matched = rd_on_subscription_matched;
    rdlistener.on_requested_deadline_missed = rd_on_requested_deadline_missed;
    rdlistener.on_requested_incompatible_qos = rd_on_requested_incompatible_qos;

    rdmask = (DDS_SAMPLE_LOST_STATUS | DDS_LIVELINESS_CHANGED_STATUS |
              DDS_SUBSCRIPTION_MATCHED_STATUS | DDS_REQUESTED_DEADLINE_MISSED_STATUS |
              DDS_REQUESTED_INCOMPATIBLE_QOS_STATUS);

    if ((q->source = DDS_Subscriber_create_datareader(data_sub, q->topic, rqos, &rdlistener, rdmask)) == NULL) {
        error("handle_create_writer: DDS_Subscriber_create_datareader failed\n");
    }
    DDS_free(rqos);
    DDS_free(tqos);
    q->source_sc = attach_reader_status_data_available(qset->ws, q->source);
}

static void handle_delete_writer(struct queueset *qset, const DDS_PublicationBuiltinTopicData *wrdesc, DDS_PublicationBuiltinTopicDataDataReader rd)
{
    struct queue *q;
    if (!accept_writer_as_queue_source(wrdesc)) {
        return;
    }
    if ((q = lookup_queue(qset, wrdesc->topic_name)) != NULL && q->source != NULL) {
        if (!have_known_matching_writers(rd, wrdesc->topic_name, broker_name)) {
            free_queue_source(qset, q);
            maybe_free_queue(qset, q);
        }
    }
}

static void handle_writer(struct queueset *qset, DDS_PublicationBuiltinTopicDataDataReader rd)
{
    DDS_SampleInfoSeq *iseq = DDS_SampleInfoSeq__alloc();
    DDS_sequence_DDS_PublicationBuiltinTopicData *mseq = DDS_sequence_DDS_PublicationBuiltinTopicData__alloc();
    DDS_ReturnCode_t rc;
    DDS_unsigned_long i;

    /* Garbage collect deleted writers */
    if ((rc = DDS_PublicationBuiltinTopicDataDataReader_take(rd, mseq, iseq, DDS_LENGTH_UNLIMITED, DDS_ANY_SAMPLE_STATE, DDS_ANY_VIEW_STATE, DDS_NOT_ALIVE_DISPOSED_INSTANCE_STATE)) == DDS_RETCODE_OK) {
        for (i = 0; i < iseq->_length; i++) {
            if (iseq->_buffer[i].valid_data) {
                handle_delete_writer(qset, &mseq->_buffer[i], rd);
            }
        }
    } else if (rc != DDS_RETCODE_NO_DATA) {
        error("handle_writer: DDS_PublicationBuiltinTopicDataDataReader_take failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_PublicationBuiltinTopicDataDataReader_return_loan(rd, mseq, iseq)) != DDS_RETCODE_OK) {
        error("handle_writer: DDS_PublicationBuiltinTopicDataDataReader_return_loan(1) failed (%s)\n", dds_strerror(rc));
    }

    /* Possibly create a new reader to match a new writer */
    if ((rc = DDS_PublicationBuiltinTopicDataDataReader_read(rd, mseq, iseq, DDS_LENGTH_UNLIMITED, DDS_NOT_READ_SAMPLE_STATE, DDS_NEW_VIEW_STATE, DDS_ALIVE_INSTANCE_STATE)) == DDS_RETCODE_OK) {
        for (i = 0; i < iseq->_length; i++) {
            if (iseq->_buffer[i].valid_data) {
                handle_create_writer(qset, &mseq->_buffer[i]);
            }
        }
    } else if (rc != DDS_RETCODE_NO_DATA) {
        error("handle_writer: DDS_PublicationBuiltinTopicDataDataReader_read failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_PublicationBuiltinTopicDataDataReader_return_loan(rd, mseq, iseq)) != DDS_RETCODE_OK) {
        error("handle_writer: DDS_PublicationBuiltinTopicDataDataReader_return_loan(1) failed (%s)\n", dds_strerror(rc));
    }

    DDS_free(iseq);
    DDS_free(mseq);
}

static int accept_reader_as_queue_sink(const DDS_SubscriptionBuiltinTopicData *rddesc)
{
    if (patmatch(topic_pattern, rddesc->topic_name)) {
        if (rddesc->partition.name._length == 1 && strcmp(rddesc->partition.name._buffer[0], broker_name) != 0) {
            return 1;
        }
    }
    return 0;
}

static struct queue *lookup_queue_sink(unsigned *pi, struct queueset *qset, const DDS_SubscriptionBuiltinTopicData *rddesc)
{
    struct queue *q;
    if ((q = lookup_queue(qset, rddesc->topic_name)) == NULL) {
        *pi = 0;
        return NULL;
    } else {
        unsigned i;
        for (i = 0; i < q->nsinks; i++) {
            if (strcmp(q->sinks[i].partition, rddesc->partition.name._buffer[0]) == 0) {
                break;
            }
        }
        *pi = i;
        return q;
    }
}

static void handle_create_reader(struct queueset *qset, const DDS_SubscriptionBuiltinTopicData *rddesc)
{
    DDS_DataWriterQos *wqos;
    DDS_ReturnCode_t rc;
    DDS_TopicQos *tqos;
    DDS_PublisherQos *pqos;
    struct DDS_DataWriterListener wrlistener;
    DDS_StatusMask wrmask;
    struct queue *q;
    unsigned i;

    if (!accept_reader_as_queue_sink(rddesc)) {
        return;
    }

    q = lookup_queue_sink(&i, qset, rddesc);
    if (q == NULL) {
        q = new_queue(qset, rddesc->topic_name);
    }
    if (i < q->nsinks) {
        return;
    }

    printf ("new reader %s.%s detected, creating writer\n", rddesc->topic_name, rddesc->partition.name._buffer[0]);

    i = q->nsinks++;
    q->sinks = realloc(q->sinks, q->nsinks * sizeof(*q->sinks));
    q->sinks[i].partition = strdup(rddesc->partition.name._buffer[0]);
    q->sinks[i].ninst = 0;
    if ((pqos = DDS_PublisherQos__alloc()) == NULL) {
        error("handle_create_reader: DDS_PublisherQos__alloc failed\n");
    }
    if ((rc = DDS_DomainParticipant_get_default_publisher_qos(dp, pqos)) != DDS_RETCODE_OK) {
        error("DDS_DomainParticipant_get_default_publisher_qos failed (%s)\n", dds_strerror(rc));
    }
    DDS_free(pqos->partition.name._buffer);
    pqos->partition.name._maximum = pqos->partition.name._length = 1;
    pqos->partition.name._buffer = DDS_StringSeq_allocbuf(pqos->partition.name._maximum);
    pqos->partition.name._buffer[0] = DDS_string_dup(rddesc->partition.name._buffer[0]);
    if ((q->sinks[i].pub = DDS_DomainParticipant_create_publisher(dp, pqos, NULL, DDS_STATUS_MASK_NONE)) == NULL) {
        error("handle_create_reader: DDS_DomainParticipant_create_publisher failed\n");
    }
    DDS_free(pqos);
    /* Create a matching data reader (we can just reuse the same publisher) */
    if ((tqos = DDS_TopicQos__alloc()) == NULL) {
        error("handle_create_reader: DDS_TopicQos__alloc failed\n");
    }
    if ((rc = DDS_Topic_get_qos(q->topic, tqos)) != DDS_RETCODE_OK) {
        error("handle_create_reader: DDS_Topic_get_qos failed (%s)\n", dds_strerror(rc));
    }
    if ((wqos = DDS_DataWriterQos__alloc()) == NULL) {
        error("handle_create_reader: DDS_DataWriterQos__alloc failed\n");
    }
    if ((rc = DDS_Publisher_get_default_datawriter_qos(q->sinks[i].pub, wqos)) != DDS_RETCODE_OK) {
        error("handle_create_reader: DDS_Publisher_get_default_datawriter_qos failed (%s)\n", dds_strerror(rc));
    }
    wqos->durability = tqos->durability;
    wqos->reliability = rddesc->reliability;
    wqos->reliability.synchronous = 1;
    wqos->destination_order.kind = DDS_BY_SOURCE_TIMESTAMP_DESTINATIONORDER_QOS;
    wqos->history.kind = DDS_KEEP_ALL_HISTORY_QOS;
    wqos->resource_limits.max_samples = 10;
    wqos->ownership = rddesc->ownership;
    wqos->writer_data_lifecycle.autodispose_unregistered_instances = 0;

    memset (&wrlistener, 0, sizeof (wrlistener));
    wrlistener.listener_data = new_listener_data(qset, q, i);
    wrlistener.on_offered_deadline_missed = wr_on_offered_deadline_missed;
    wrlistener.on_liveliness_lost = wr_on_liveliness_lost;
    wrlistener.on_publication_matched = wr_on_publication_matched;
    wrlistener.on_offered_incompatible_qos = wr_on_offered_incompatible_qos;

    wrmask = (DDS_LIVELINESS_LOST_STATUS | DDS_OFFERED_DEADLINE_MISSED_STATUS |
              DDS_PUBLICATION_MATCHED_STATUS | DDS_OFFERED_INCOMPATIBLE_QOS_STATUS);

    if ((q->sinks[i].wr = DDS_Publisher_create_datawriter(q->sinks[i].pub, q->topic, wqos, &wrlistener, wrmask)) == NULL) {
        error("handle_create_reader: DDS_Publisher_create_datawriter failed\n");
    }
    DDS_free(wqos);
    DDS_free(tqos);

    if (q->instmap) {
        rebalance_new_sink(q);
    }
}

static void handle_delete_reader(struct queueset *qset, const DDS_SubscriptionBuiltinTopicData *rddesc, DDS_SubscriptionBuiltinTopicDataDataReader rd)
{
    struct queue *q;
    unsigned i;
    if (!accept_reader_as_queue_sink(rddesc)) {
        return;
    }
    if ((q = lookup_queue_sink(&i, qset, rddesc)) != NULL && i < q->nsinks) {
        if (!have_known_matching_readers(rd, rddesc->topic_name, rddesc->partition.name._buffer[0])) {
            free_queue_sink(q, i);
            maybe_free_queue(qset, q);
        }
    }
}

static void handle_reader(struct queueset *qset, DDS_SubscriptionBuiltinTopicDataDataReader rd)
{
    DDS_SampleInfoSeq *iseq = DDS_SampleInfoSeq__alloc();
    DDS_sequence_DDS_SubscriptionBuiltinTopicData *mseq = DDS_sequence_DDS_SubscriptionBuiltinTopicData__alloc();
    DDS_ReturnCode_t rc;
    DDS_unsigned_long i;

    /* Garbage collect deleted readers */
    if ((rc = DDS_SubscriptionBuiltinTopicDataDataReader_take(rd, mseq, iseq, DDS_LENGTH_UNLIMITED, DDS_ANY_SAMPLE_STATE, DDS_ANY_VIEW_STATE, DDS_NOT_ALIVE_DISPOSED_INSTANCE_STATE)) == DDS_RETCODE_OK) {
        for (i = 0; i < iseq->_length; i++) {
            if (iseq->_buffer[i].valid_data) {
                handle_delete_reader(qset, &mseq->_buffer[i], rd);
            }
        }
    } else if (rc != DDS_RETCODE_NO_DATA) {
        error("handle_reader: DDS_SubscriptionBuiltinTopicDataDataReader_take failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_SubscriptionBuiltinTopicDataDataReader_return_loan(rd, mseq, iseq)) != DDS_RETCODE_OK) {
        error("handle_reader: DDS_SubscriptionBuiltinTopicDataDataReader_return_loan(1) failed (%s)\n", dds_strerror(rc));
    }

    /* Possibly create a new writer to match a new reader */
    if ((rc = DDS_SubscriptionBuiltinTopicDataDataReader_read(rd, mseq, iseq, DDS_LENGTH_UNLIMITED, DDS_NOT_READ_SAMPLE_STATE, DDS_NEW_VIEW_STATE, DDS_ALIVE_INSTANCE_STATE)) == DDS_RETCODE_OK) {
        for (i = 0; i < iseq->_length; i++) {
            if (iseq->_buffer[i].valid_data) {
                handle_create_reader(qset, &mseq->_buffer[i]);
            }
        }
    } else if (rc != DDS_RETCODE_NO_DATA) {
        error("handle_reader: DDS_SubscriptionBuiltinTopicDataDataReader_read failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_SubscriptionBuiltinTopicDataDataReader_return_loan(rd, mseq, iseq)) != DDS_RETCODE_OK) {
        error("handle_reader: DDS_SubscriptionBuiltinTopicDataDataReader_return_loan(1) failed (%s)\n", dds_strerror(rc));
    }

    DDS_free(iseq);
    DDS_free(mseq);
}

static void rebalance_new_sink(struct queue *q)
{
    struct hh_iter it;
    struct instmap_entry *x;
    unsigned ntgt;
    assert(q->nsinks > 0);
    assert(q->cursor < q->nsinks);
    ntgt = q->ninst / q->nsinks;
    for (x = hh_iter_first(q->instmap, &it); x && ntgt != 0; x = hh_iter_next(&it)) {
        assert(x->idx < q->nsinks - 1);
        assert(q->sinks[x->idx].ninst > 0);
        if (++q->cursor == q->nsinks) {
            q->sinks[q->nsinks - 1].ninst++;
            q->sinks[x->idx].ninst--;
            x->idx = q->nsinks - 1;
            q->cursor = 0;
        }
    }
}

static void rebalance_free_sink(struct queue *q, unsigned idx)
{
    struct hh_iter it;
    struct instmap_entry *x;
    assert(q->cursor < q->nsinks || (q->cursor == 0 && q->nsinks == 0));
    for (x = hh_iter_first(q->instmap, &it); x; x = hh_iter_next(&it)) {
        if (x->idx == idx) {
            x->idx = q->cursor;
            if (q->nsinks > 0) {
                q->cursor = (q->cursor + 1) % q->nsinks;
            }
        } else if (x->idx == q->nsinks) {
            x->idx = idx;
        }
        q->sinks[x->idx].ninst++;
    }
}

static unsigned map_instance_handle(struct queue *q, DDS_InstanceHandle_t handle)
{
    struct instmap_entry tmp, *x;
    tmp.handle = handle;
    if ((x = hh_lookup(q->instmap, &tmp)) == NULL) {
        tmp.idx = q->cursor;
        q->cursor = (q->cursor + 1) % q->nsinks;
        hh_add(q->instmap, &tmp);
        return tmp.idx;
    } else {
        if (x->idx >= q->nsinks) {
            x->idx = q->cursor;
            hh_add(q->instmap, &tmp);
            q->cursor = (q->cursor + 1) % q->nsinks;
        }
        return x->idx;
    }
}

static void forward_data (struct queue *q, const DDS_SampleInfo *info, const void *data)
{
    if (q->cursor < q->nsinks) {
        DDS_ReturnCode_t rc;
        unsigned idx;

        if (q->is_keyless) {
            idx = q->cursor;
            q->cursor = (q->cursor + 1) % q->nsinks;
        } else {
            idx = map_instance_handle(q, info->instance_handle);
        }

        if ((rc = DDS_DataWriter_write_w_timestamp(q->sinks[idx].wr, (void *) data, DDS_HANDLE_NIL, &info->source_timestamp)) != DDS_RETCODE_OK) {
            error("forward_data: DDS_DataWriter_write_w_timestamp failed (%s)\n", dds_strerror(rc));
        }
        /* FIXME: unregister, require keyless topics, unregister only for non-keyless topics, or ... ? */
    }
}

static void handle_data (struct queue *q)
{
    DDS_SampleInfoSeq *iseq = DDS_SampleInfoSeq__alloc();
    DDS_octSeq *mseq = DDS_octSeq__alloc();
    DDS_ReturnCode_t rc;
    do {
        if ((rc = DDS_DataReader_take(q->source, (DDS_sequence) mseq, iseq, 1, DDS_ANY_SAMPLE_STATE, DDS_ANY_VIEW_STATE, DDS_ANY_INSTANCE_STATE)) == DDS_RETCODE_OK) {
            assert(iseq->_length == 1);
            if (iseq->_buffer[0].valid_data) {
                forward_data(q, &iseq->_buffer[0], mseq->_buffer);
            }
            if ((rc = DDS_DataReader_return_loan(q->source, (DDS_sequence) mseq, iseq)) != DDS_RETCODE_OK) {
                error("handle_data: DDS_DataReader_return_loan failed (%s)\n", dds_strerror(rc));
            }
        }
    } while (rc == DDS_RETCODE_OK);
    if (rc != DDS_RETCODE_NO_DATA) {
        /* No data is technically possible when the writer used a lifespan ... */
        error("handle_data: DDS_DataReader_take failed (%s)\n", dds_strerror(rc));
    }
    DDS_free(iseq);
    DDS_free(mseq);
}

int main(int argc, char **argv)
{
    DDS_DomainParticipantFactory dpf;
    DDS_Subscriber builtin_sub;
    DDS_PublicationBuiltinTopicDataDataReader builtin_rd_w;
    DDS_PublicationBuiltinTopicDataDataReader builtin_rd_r;
    DDS_StatusCondition new_wr_sc;
    DDS_StatusCondition new_rd_sc;
    DDS_SubscriberQos *sqos;
    DDS_ReturnCode_t rc;
    struct queueset qset;
    int terminate = 0;
    int opt;
#ifndef _WIN32
    pthread_t sigtid;
#endif

    saved_argv0 = argv[0];

    while ((opt = getopt (argc, argv, "1")) != EOF) {
        switch (opt) {
            case '1':
                keyless_mode_flag = 1;
                break;
            default:
                usage();
        }
    }
    if (optind + 2 != argc ) {
        usage();
    }
    broker_name = argv[optind];
    topic_pattern = argv[optind+1];

    printf("keyless=%d partition=%s topic=%s\n", keyless_mode_flag, broker_name, topic_pattern);

    qset.queues = NULL;

    if ((dpf = DDS_DomainParticipantFactory_get_instance()) == NULL) {
        error("DDS_DomainParticipantFactory_get_instance failed\n");
    }
    if ((dp = DDS_DomainParticipantFactory_create_participant(dpf, DDS_DOMAIN_ID_DEFAULT, DDS_PARTICIPANT_QOS_DEFAULT, NULL, DDS_STATUS_MASK_NONE)) == NULL) {
        error("DDS_DomainParticipantFactory_create_participant failed\n");
    }
    if ((termcond = DDS_GuardCondition__alloc()) == NULL) {
        error("DDS_GuardCondition__alloc(termcond) failed\n");
    }
    if ((sqos = DDS_SubscriberQos__alloc()) == NULL) {
        error("DDS_SubscriberQos__alloc failed\n");
    }
    if ((rc = DDS_DomainParticipant_get_default_subscriber_qos(dp, sqos)) != DDS_RETCODE_OK) {
        error("DDS_DomainParticipant_get_default_subscriber_qos failed (%s)\n", dds_strerror(rc));
    }
    DDS_free(sqos->partition.name._buffer);
    sqos->partition.name._maximum = sqos->partition.name._length = 1;
    sqos->partition.name._buffer = DDS_StringSeq_allocbuf(sqos->partition.name._maximum);
    sqos->partition.name._buffer[0] = DDS_string_dup(broker_name);
    if ((data_sub = DDS_DomainParticipant_create_subscriber(dp, sqos, NULL, DDS_STATUS_MASK_NONE)) == NULL) {
        error("DDS_DomainParticipant_create_subscriber failed\n");
    }
    DDS_free(sqos);
    if ((builtin_sub = DDS_DomainParticipant_get_builtin_subscriber(dp)) == NULL) {
        error("DDS_DomainParticipant_get_builtin_subscriber failed\n");
    }
    if ((builtin_rd_w = DDS_Subscriber_lookup_datareader(builtin_sub, "DCPSPublication")) == NULL) {
        error("DDS_Subscriber_lookup_datareader(DCPSPublication) failed\n");
    }
    if ((builtin_rd_r = DDS_Subscriber_lookup_datareader(builtin_sub, "DCPSSubscription")) == NULL) {
        error("DDS_Subscriber_lookup_datareader(DCPSSubscription) failed\n");
    }
    if ((qset.ws = DDS_WaitSet__alloc()) == NULL) {
        error("DDS_WaitSet__alloc failed\n");
    }
    if ((rc = DDS_WaitSet_attach_condition(qset.ws, termcond)) != DDS_RETCODE_OK) {
        error("DDS_WaitSet_attach_condition(termcond) failed\n");
    }
    new_wr_sc = attach_reader_status_data_available(qset.ws, builtin_rd_w);
    new_rd_sc = attach_reader_status_data_available(qset.ws, builtin_rd_r);

#ifndef _WIN32
    signal(SIGINT, sigh);
    signal(SIGTERM, sigh);
    if (pipe (sigpipe) != 0) {
        error("pipe(sigpipe): errno %d\n", errno);
    }
    pthread_create(&sigtid, NULL, sigthread, NULL);
#else
    termev = CreateEvent(NULL, TRUE, FALSE, NULL);
    SetConsoleCtrlHandler(console_ctrl_handler, TRUE);
#endif

    while (!terminate) {
        DDS_ConditionSeq *conds = DDS_ConditionSeq__alloc();
        DDS_Duration_t timeout = DDS_DURATION_INFINITE;
        DDS_unsigned_long i;
        if ((rc = DDS_WaitSet_wait(qset.ws, conds, &timeout)) != DDS_RETCODE_OK) {
            error("DDS_WaitSet_wait failed (%s)\n", dds_strerror(rc));
        }
        for (i = 0; i < conds->_length && !terminate; i++) {
            if (conds->_buffer[i] == termcond) {
                terminate = 1;
            } if (conds->_buffer[i] == new_wr_sc) {
                handle_writer(&qset, builtin_rd_w);
            } else if (conds->_buffer[i] == new_rd_sc) {
                handle_reader(&qset, builtin_rd_r);
            } else {
                struct queue *q;
                for (q = qset.queues; q != NULL; q = q->next) {
                    if (conds->_buffer[i] == q->source_sc) {
                        handle_data(q);
                        break;
                    }
                }
            }
        }
        DDS_free(conds);
    }

#ifndef _WIN32
    {
        const char c = 0;
        size_t i;
        write(sigpipe[1], &c, 1);
        pthread_join(sigtid, NULL);
        for(i = 0; i < sizeof(sigpipe)/sizeof(sigpipe[0]); i++) {
            close(sigpipe[i]);
        }
    }
#endif

    while (qset.queues != NULL)
    {
        struct queue *q = qset.queues;
        if (q->source) {
            free_queue_source (&qset, q);
        }
        while (q->nsinks > 0) {
            free_queue_sink (q, 0);
        }
        maybe_free_queue (&qset, q);
    }

    if ((rc = DDS_WaitSet_detach_condition(qset.ws, new_rd_sc)) != DDS_RETCODE_OK) {
        error("DDS_WaitSet_detach_condition(readers) failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_WaitSet_detach_condition(qset.ws, new_wr_sc)) != DDS_RETCODE_OK) {
        error("DDS_WaitSet_detach_condition(writers) failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_WaitSet_detach_condition(qset.ws, termcond)) != DDS_RETCODE_OK) {
        error("DDS_WaitSet_detach_condition(termcond) failed (%s)\n", dds_strerror(rc));
    }
    DDS_free(qset.ws);
    DDS_free(termcond);

    if ((rc = DDS_DomainParticipant_delete_contained_entities(dp)) != DDS_RETCODE_OK) {
        error("DDS_DomainParticipant_delete_contained_entities failed (%s)\n", dds_strerror(rc));
    }
    if ((rc = DDS_DomainParticipantFactory_delete_participant(dpf, dp)) != DDS_RETCODE_OK) {
        error("DDS_DomainParticipantFactory_delete_participant failed (%s)\n", dds_strerror(rc));
    }
#ifdef _WIN32
    printf("setevent termev\n");
    SetEvent(termev);
#endif
    return 0;
}
