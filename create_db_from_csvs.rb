require 'active_record'
require 'qx'
require 'pry'
require 'csv'

module CSVDB

  def self.load_from_directory(dir_name)
    db_url = "postgres://admin:password@localhost/cc_import"
    ActiveRecord::Base.establish_connection(db_url)
    tm = PG::BasicTypeMapForResults.new(ActiveRecord::Base.connection.raw_connection)
    Qx.config(type_map: tm)
    Qx.transaction do
      Qx.execute("DROP SCHEMA IF EXISTS public CASCADE")
      Qx.execute("CREATE SCHEMA public")
      to_insert = []
      Dir.glob(dir_name + '/*.csv') do |path|
        next if path =~ /^ignore_.*$/
        table_name = File.basename(path, '.csv')
        csv = []
        CSV.foreach(path, headers: true, encoding: 'ISO-8859-1'){|row| csv.push(row.to_h)}
        headers = csv.first.keys
        puts "creating #{table_name} with #{headers}"
        Qx.execute("CREATE TABLE \"#{table_name}\" (#{headers.map{|h| "\"#{h}\" text"}.join(",")})")
        Qx.insert_into(table_name).values(csv).execute
      end
    end
  end
end

# Given a directory full of CSVs,
# where each filename is a tablename
# and each header is a column name
#
# Create each of those tables with each of those columns (all text type cols)
# Use COPY to import all the rows from all the files into their corresponding tables


