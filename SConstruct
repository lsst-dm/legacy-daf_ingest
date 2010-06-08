# -*- python -*-
#
# Setup our environment
#
import os.path, re, os
import lsst.SConsUtils as scons

env = scons.makeEnv("datarel",
                    r"$HeadURL$",
                    []
                    )
#
# Build/install things
#
for d in Split("doc tests"):
    SConscript(os.path.join(d, "SConscript"))

env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

Alias("install", env.Install(env['prefix'], "bin"))
Alias("install", env.Install(env['prefix'], "etc"))
Alias("install", env.Install(env['prefix'], "pipeline"))
Alias("install", env.Install(env['prefix'], "python"))
Alias("install", env.InstallEups(os.path.join(env['prefix'], "ups")))

scons.CleanTree(r"*~ core *.so *.os *.o")
#
# Build TAGS files
#
files = scons.filesToTag()
if files:
    env.Command("TAGS", files, "etags -o $TARGET $SOURCES")

env.Declare()
env.Help("""
LSST date release package
""")

