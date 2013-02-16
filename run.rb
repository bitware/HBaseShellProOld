#! /usr/bin/env ruby
#
# Tool for running java program, class files
# will be built automatically if necessary
#
# Usage:
#  > ruby ruby.rb [parameters_passed_to_java_program]
#

require 'find'
require 'fileutils'

# basic directories
SRC_DIR = 'src'
LIB_DIR = 'lib'
BIN_DIR = 'bin'

# all pathes are based on THIS_FILE_PATH
THIS_FILE_PATH  = File.expand_path(__FILE__)
THIS_FILE_DIR   = File.dirname(THIS_FILE_PATH)

# main class related constances
MAIN_CLASS_NAME = File.basename(THIS_FILE_DIR)
MAIN_CLASS_FILE = "#{BIN_DIR}/main/#{MAIN_CLASS_NAME}.class"
MAIN_CLASS      = "main.#{MAIN_CLASS_NAME}"

# platform(linux or windows) related constances
IS_LINUX        = File.exist?('/dev/null')
CLASSPATH_JOIN  = IS_LINUX ? ':' : ';'

def exec_cmd(cmd)
    # open the following comment if commands are needed
    # puts cmd
    system cmd
end

def get_files(dir, type)
    files = []

    if File.exist?(dir)
        Find.find(dir) { |file|
            files << file if file =~ /\.#{type}$/
        }
    end

    return files
end

def newest_modified_time_of(files)
    newest_modified_time = 0

    files.each { |file|
        mtime = File.mtime(file).to_i
        newest_modified_time = mtime if mtime > newest_modified_time
    }

    return newest_modified_time
end

def class_files_out_of_date(source_files, class_files)
    newest_modified_time_of(source_files) > newest_modified_time_of(class_files)
end

def main_class_file_not_found()
    !File.exists?(MAIN_CLASS_FILE)
end

def run_java()
    # get files
    source_files = get_files(SRC_DIR, 'java')
    jar_files    = get_files(LIB_DIR, 'jar')
    class_files  = get_files(BIN_DIR, 'class')

    # classpath
    classpath_for_compile = (jar_files + Array(SRC_DIR)).join(CLASSPATH_JOIN)
    classpath_for_run     = (jar_files + Array(BIN_DIR)).join(CLASSPATH_JOIN)

    compiled_ok = true

    # build class files if necessary
    if main_class_file_not_found || class_files_out_of_date(source_files, class_files)
        FileUtils.mkpath BIN_DIR
        compiled_ok = exec_cmd "javac -encoding UTF-8 -d #{BIN_DIR} -classpath #{classpath_for_compile} #{source_files.join(' ')}"
    end

    # run java program
    exec_cmd "java -classpath #{classpath_for_run} #{MAIN_CLASS} #{ARGV.join(' ')}" if compiled_ok
end

def main()
    current_dir = Dir.pwd

    Dir.chdir THIS_FILE_DIR
    run_java
    Dir.chdir current_dir
end

# run main
main
