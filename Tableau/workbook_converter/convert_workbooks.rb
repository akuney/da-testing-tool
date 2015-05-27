require 'fileutils'

SOURCE_DIR = File.join File.dirname(__FILE__), '../Workbooks'
DESTINATION_DIR = File.join File.dirname(__FILE__), 'out'

def replace_vertica_version text
  text.gsub(/expected-driver-version='[^']*'/,"expected-driver-version='7.0'")
end

def replace_connect_string_extras text
  text.gsub(/odbc-connect-string-extras='[^']*'/,"odbc-connect-string-extras='ConnectionLoadBalance=1'")
end

def replace_vertica_username text
  text.gsub(/server='[^']*'/,"server='10.0.2.2'")
end

def replace_vertica_host text
  text.gsub(/username='tableau'/, "username='dbadmin'")
end

def replace_production_schema text
  text.gsub(/intent_media_production/, 'intent_media_development')
end

def replace_mysql_username text
  modified_lines = text.lines.collect do |line|
    if line.include?('connection') and line.include?('mysql')
      line.gsub(/username='[^']*'/,"username='root'")
    else
      line
    end
  end
      
  modified_lines.join
end

def replace_extract_location text
  modified_lines = text.lines.collect do |line|
    if line.include?('connection') and line.include?('dataengine')
      filepath = /dbname='([^']*)'/.match(line)[1]
      filename = File.basename(filepath)
      line.gsub(/dbname='[^']*'/, "dbname='c:\\tableau_cache\\#{filename}'")
    else
      line
    end
  end
      
  modified_lines.join
end

def disable_extracts text
  modified_lines = text.lines.collect do |line|
    if line.include?('extract') and line.include?('enabled=')
      line.gsub(/enabled='[^']*'/,"enabled='false'")
    else
      line
    end
  end
      
  modified_lines.join
end

def all_workbooks
  Dir.glob(SOURCE_DIR + "/**/*.twb")
end

def convert
  all_workbooks.each do | workbook_path |
    modified_workbook = File.read workbook_path

    modified_workbook = replace_vertica_version modified_workbook
    modified_workbook = replace_connect_string_extras modified_workbook
    #modified_workbook = replace_vertica_host modified_workbook
    #modified_workbook = replace_vertica_username modified_workbook
    #modified_workbook = replace_production_schema modified_workbook
    #modified_workbook = replace_mysql_username modified_workbook
    #modified_workbook = replace_extract_location modified_workbook
    #modified_workbook = disable_extracts modified_workbook

    
    full_dir, filename = File.split(workbook_path)
    base_dir, site_dir = File.split(full_dir)
    FileUtils.mkpath File.join(DESTINATION_DIR, site_dir)
    modified_file = File.new File.join(DESTINATION_DIR, site_dir, filename), 'w'
    modified_file.write modified_workbook
  end
end


convert


