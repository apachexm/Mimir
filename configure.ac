# initialize
AC_INIT([mimir], [0.9], [taogao@udel.edu])
AM_INIT_AUTOMAKE([foreign -Wall -Werror])

# macro directory
AC_CONFIG_MACRO_DIR([m4])

# configure files
AC_CONFIG_HEADERS([src/ac_config.h])
AC_CONFIG_FILES([Makefile src/Makefile examples/Makefile generator/Makefile test/Makefile])

# check compiler
AM_PROG_AR
AC_PROG_CXX(mpicxx)
AX_CXX_COMPILE_STDCXX_11
AC_PROG_RANLIB

# check memkind library
AC_CHECK_LIB(memkind, hbw_posix_memalign, [], [], [])

# check papi library
AC_ARG_WITH([papi],
            AS_HELP_STRING([--with-papi], [papi directory]),
            [without_papi=no], [without_papi=yes])

AS_IF([test "x$without_papi" != xyes],
      [CPPFLAGS="-I$with_papi/include $CPPFLAGS"], [])
AS_IF([test "x$without_papi" != xyes],
      [LDFLAGS="-L$with_papi/lib $LDFLAGS"], [])

AS_IF([test "x$without_papi" != xyes],
      [AC_CHECK_LIB(papi, PAPI_library_init)],[])

AC_OUTPUT
