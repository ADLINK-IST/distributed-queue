#
# PrismTech licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with the
# License and with the PrismTech Vortex product. You may obtain a copy of the
# License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License and README for the specific language governing permissions and
# limitations under the License.

# Target operating system and processor matter: we pick the compiler
# based on the platform and we set the 32/64-bit flags for
# x86/x86_64. A "properly installed" OpenSplice has the platform as
# the last bit of OSPL_HOME, but building against a checked-out source
# tree requires looking at SPLICE_TARGET.
ifneq "$(SPLICE_TARGET)" ""
OS_PROC := $(SPLICE_TARGET)
else
OS_PROC := $(notdir $(OSPL_HOME))
endif
OS := $(shell echo $(OS_PROC) | sed -e 's/^[^.]*\.//' -e 's/^\([^-_]*\)[-_].*/\1/')
PROC := $(shell echo $(OS_PROC) | sed -e 's/^\([^.]*\)\..*/\1/')

ifneq "$(filter darwin%, $(OS))" "" # MacOS X
  CC = clang
  CXX = clang++
  LD = $(CC)
  LDXX = $(CXX)
  OPT = -O3 -DNDEBUG
  PROF = 
  CFLAGS += -Wall -g $(OPT) $(PROF) #-fno-inline
  CXXFLAGS += $(CFLAGS) -std=c++11 -DOSPL_USE_CXX11
  LDFLAGS += -g $(OPT) $(PROF)
  X =
  O = .o
  A = .a
else
  ifneq "$(filter win%, $(OS))" "" # Windows, assumes VS2013
    OS = win32 # win64 platform code uses win32 in path names
    CC = cl
    CXX = cl
    LD = link
    LDXX = link
    OPT = -O2 -DNDEBUG
    PROF =
    CFLAGS = -Zi -W3 $(OPT) $(PROF) -TC -bigobj
    CXXFLAGS = $(filter-out -TC, $(CFLAGS)) -EHsc -TP
    LDFLAGS += -nologo -incremental:no -subsystem:console -debug
    X = .exe
    O = .obj
    A = .lib
    #VS_HOME=/cygdrive/C/Program Files (x86)/Microsoft Visual Studio 10.0
    #WINDOWSSDKDIR=/cygdrive/C/Program Files (x86)/Microsoft SDKs/Windows/v7.0A
    VS_HOME=/cygdrive/C/Program Files (x86)/Microsoft Visual Studio 12.0
    WINDOWSSDKDIR=/cygdrive/C/Program Files (x86)/Microsoft SDKs/Windows/v7.1A
  else # assume Linux (mostly Unix with gcc)
    CC = gcc
    CXX = g++
    LD = $(CC)
    LDXX = $(CXX)
    OPT = -O3 -DNDEBUG
    PROF =
    CFLAGS += -Wall $(OPT) $(PROF)
    CXXFLAGS += $(CFLAGS) -std=gnu++0x
    LDFLAGS += $(OPT) $(PROF)
    X =
    O = .o
    A = .a
  endif
endif

# We're assuming use of cygwin, which means Windows path names can be
# obtained using "cygpath". With "-m" we get slashes (rather than
# backslashes), which all of MS' tools accept and which are far less
# troublesome in make.
ifeq "$(CC)" "cl"
  N_OSPL_HOME := $(shell cygpath -m '$(OSPL_HOME)')
  N_OSPL_OUTER_HOME := $(shell cygpath -m '$(OSPL_OUTER_HOME)')
  N_VS_HOME := $(shell cygpath -m '$(VS_HOME)')
  N_WINDOWSSDKDIR := $(shell cygpath -m '$(WINDOWSSDKDIR)')
else # not Windows
  N_OSPL_HOME := $(OSPL_HOME)
  N_OSPL_OUTER_HOME := $(OSPL_OUTER_HOME)
endif

# More machine- and platform-specific matters.
ifeq "$(CC)" "cl" # Windows
  ifeq "$(PROC)" "x86_64"
    MACHINE = -machine:X64
  endif
  LDFLAGS += $(MACHINE)
  OBJ_OFLAG = -Fo
  EXE_OFLAG = -out:
  define make_archive
	lib $(MACHINE) /out:$@ $^
  endef
  CPPFLAGS += -D_CRT_SECURE_NO_WARNINGS
  # This works for VS2010, VS2013
  CPPFLAGS += '-I$(N_VS_HOME)/VC/include' '-I$(N_WINDOWSSDKDIR)/Include'
  ifeq "$(PROC)" "x86_64"
    LDFLAGS += '-libpath:$(N_VS_HOME)/VC/lib/amd64' '-libpath:$(N_WINDOWSSDKDIR)/lib/x64'
  else
    LDFLAGS += '-libpath:$(N_VS_HOME)/VC/lib' '-libpath:$(N_WINDOWSSDKDIR)/lib'
  endif
else # not Windows
  OBJ_OFLAG = -o
  EXE_OFLAG = -o 
  define make_archive
	ar -ru $@ $?
  endef
  ifeq "$(PROC)" "x86"
    CFLAGS += -m32
    LDFLAGS += -m32
  endif
  ifeq "$(PROC)" "x86_64"
    CFLAGS += -m64
    LDFLAGS += -m64
  endif
endif

# If $(SPLICE_TARGET) is set, assume we're building against a source
# tree - add just about every place something of use may be found to
# the include path, especially considering that it gets double if both
# outer- & inner-ring sources are available.
ifneq "$(SPLICE_TARGET)" ""
  OSPL_INTERNAL_INC = user/include user/code abstraction/os-net/include abstraction/os-net/$(OS)
  OSPLINC = $(addprefix src/, api/dcps/sac/bld/$(SPLICE_TARGET) api/dcps/sac/include database/database/include database/serialization/include kernel/include osplcore/bld/$(SPLICE_TARGET) abstraction/os/include abstraction/os/$(OS) $(OSPL_INTERNAL_INC)) examples/include
  OSPLINC_SACPP = $(addprefix src/, api/streams/c++/sacpp/include api/streams/c++/ccpp/include api/dcps/c++/sacpp/bld/$(SPLICE_TARGET) api/dcps/common/include api/dcps/c++/sacpp/include api/dcps/c++/common/include) $(filter-out src/api/dcps/sac/include, $(OSPLINC))
  OSPLINC_ISOCPP = $(addprefix src/, api/dcps/isocpp/include api/dcps/c++/sacpp/include api/dcps/c++/common/include)
  IDLPP_INC = -I$(N_OSPL_HOME)/etc/idl
else
  OSPLINC = include/dcps/C/SAC
  OSPLINC_SACPP = include/dcps/C++/SACPP
  OSPLINC_ISOCPP = include/dcps/C++/isocpp
  IDLPP_INC =
endif

ifeq "$(CC)" "cl"
  LDFLAGS += -libpath:$(N_OSPL_HOME)/lib/$(SPLICE_TARGET)
  LIBDEP_SYS = kernel32 ws2_32
  LIBDEP_PRE =
  LIBDEP_SUF = .lib
else
  LDFLAGS += -L$(N_OSPL_HOME)/lib/$(SPLICE_TARGET)
  LIBDEP_SYS =
  LIBDEP_PRE = -l
  LIBDEP_SUF =
endif

.PHONY: all clean
.PRECIOUS: %
.SECONDARY:

%$O: %.c
%$O: %.cpp
%$X:

IDL_GENDIR = gen
LANG = c

ifeq "$(LANG)" "c"
  idlSRC = $(addprefix $(IDL_GENDIR)/, $1SplDcps.c $1SacDcps.c)
  idlH = $(addprefix $(IDL_GENDIR)/, $1.h $1Dcps.h $1SplDcps.h $1SacDcps.h)
  idlX = $(patsubst %.c, %$O, $(sort $(foreach x, $1, $(call idlSRC, $x))))
  LDLIBS += $(patsubst %, $(LIBDEP_PRE)%$(LIBDEP_SUF), dcpssac ddskernel $(LIBDEP_SYS))
  CPPFLAGS += $(OSPLINC:%=-I$(N_OSPL_HOME)/%)
  ifneq "$(N_OSPL_OUTER_HOME)" ""
    CPPFLAGS += $(OSPLINC:%=-I$(N_OSPL_OUTER_HOME)/%)
  endif
else
  ifeq "$(LANG)" "cpp"
    idlSRC = $(addprefix $(IDL_GENDIR)/, $1.cpp $1Dcps.cpp $1SplDcps.cpp $1Dcps_impl.cpp)
    idlH = $(addprefix $(IDL_GENDIR)/, ccpp_$1.h $1.h $1Dcps.h $1SplDcps.h $1Dcps_impl.h)
    idlX = $(patsubst %.cpp, %$O, $(sort $(foreach x, $1, $(call idlSRC, $x))))
    LDLIBS += $(patsubst %, $(LIBDEP_PRE)%$(LIBDEP_SUF), dcpssacpp ddskernel $(LIBDEP_SYS))
    CPPFLAGS += $(OSPLINC_SACPP:%=-I$(N_OSPL_HOME)/%)
    ifneq "$(N_OSPL_OUTER_HOME)" ""
      CPPFLAGS += $(OSPLINC_SACPP:%=-I$(N_OSPL_OUTER_HOME)/%)
    endif
  else
    ifeq "$(LANG)" "isocpp"
      idlSRC = $(addprefix $(IDL_GENDIR)/, $1.cpp $1Dcps.cpp $1SplDcps.cpp $1Dcps_impl.cpp)
      idlH = $(addprefix $(IDL_GENDIR)/, $1_DCPS.hpp $1.h $1Dcps.h $1SplDcps.h $1Dcps_impl.h)
      idlX = $(patsubst %.cpp, %$O, $(sort $(foreach x, $1, $(call idlSRC, $x))))
      LDLIBS += $(patsubst %, $(LIBDEP_PRE)%$(LIBDEP_SUF), dcpsisocpp dcpssacpp ddskernel $(LIBDEP_SYS))
      CPPFLAGS += $(OSPLINC_SACPP:%=-I$(N_OSPL_HOME)/%)
      CPPFLAGS += $(OSPLINC_ISOCPP:%=-I$(N_OSPL_HOME)/%)
    else
      $(error $$(LANG) not set to c, cpp or isocpp)
    endif
  endif
endif

PROGS = dqbroker
#IDL_dqbroker = aaa

SRCDIR = .
CPPFLAGS += -I$(IDL_GENDIR)
vpath %.idl $(SRCDIR)/IDL

all: $(PROGS:%=%$X)

%$X: %$O
	$(LDXX) $(LDXXFLAGS) $(LDFLAGS) $(EXE_OFLAG)$@ $^ $(LDLIBS)

%$O: %.c | idlpp
	$(CC) $(CPPFLAGS) $(CFLAGS) $(OBJ_OFLAG)$@ -c $<

%$O: %.cpp | idlpp
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) $(OBJ_OFLAG)$@ -c $<

dqbroker$X: $(call idlX, $(IDL_dqbroker))

$(call idlH, %) $(call idlSRC, %): %.idl
	idlpp -S -lisoc++ $(IDLPP_INC) -d$(IDL_GENDIR) $<

.PHONY: idlpp
idlpp: $(foreach x, $(sort $(foreach y, $(PROGS), $(IDL_$y))), $(call idlH, $x))

clean:
	rm -rf $(PROGS:%=%$X) *$O $(IDL_GENDIR)
