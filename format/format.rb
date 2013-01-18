# format source files using uncrustify
# files in current directory will be processed if no args
#
# Usage:
#   > ruby format.rb [folders or files list]
#
# Example:
#   > ruby format.rb
#   > ruby format.rb ./src
#   > ruby format.rb ./src tmp.java

require 'find'
require 'open3'

# if ignore_svn_flg = true, all files will be processed
# otherwise, only files with 'M' or 'A' svn flag will be processed
$ignore_svn_flag = false

def is_linux?()
    File.exist?('/dev/null')
end

this_dir = File.dirname(__FILE__)

$config_file = this_dir + '/uncrustify_java.cfg'
$uncrustify  = this_dir + '/uncrustify'
$uncrustify  += '.exe' if !is_linux?        # Windows

def filter_svn(file)
    stdin, stdout, stderr = Open3.popen3("svn status #{file}")
    status = stdout.readlines[0]
    return false if !status

    puts status
    flag = status[0]
    flag == 'M' || flag == 'A'
end

def filter(file)
    return false if File.extname(file).casecmp('.java') != 0
    $ignore_svn_flag || filter_svn(file)
end

def format(file)
    `#{$uncrustify} -l JAVA -c #{$config_file} --no-backup #{file}` if filter(file)
end

def traval(files)
    files.each { |file|
        Find.find(file) { |f|
            if File.directory?(f)
                File.basename(f) == '.svn' ? Find.prune : next
            end

            format(f)
        }
    }
end

files = (ARGV.length > 0) ? ARGV : ['.']
traval(files)
