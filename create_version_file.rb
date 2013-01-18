#! /usr/bin/env ruby
#
# Tool for creating svn version file
#
# Usage:
#  > ruby create_version.rb
#

VERSION_FILE_PATH = 'src/main/Version.java'

def version()
    svnversion = `svnversion -n`
    version_num = svnversion.to_i

    if version_num.to_s != svnversion
        version_num += 1
    end

    sprintf("%04d", version_num)
end

def build_time()
    Time.now
end

def create_version_file()
    open(VERSION_FILE_PATH, 'w') { |f|
        f << "package main;\n"
        f << "\n"
        f << "public final class Version {\n"
        f << "    public static final String REVISION   = \"#{version}\";\n"
        f << "    public static final String BUILD_TIME = \"#{build_time}\";\n"
        f << "}\n"
    }
end

create_version_file
