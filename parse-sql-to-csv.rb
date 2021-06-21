require 'csv'

fileName = $ARGV[0]
SQL_STMT_COL = 4
ES_STMT = 5
FILE=1


outputFileName = "#{File.basename(fileName,".csv")}-output.csv"

DML= 'INSERT|DELETE|UPDATE|TRUNCATE|REPLACE'
DML_STMT_REGEXP= Regexp.new "(#{DML})"
STATEMENT_TOKEN_REGEXP=Regexp.new "^(CREATE|ALTER|DROP|#{DML}|SELECT|SHOW)"
SELECT_QUERY_REGEXP = Regexp.new "^SELECT"
PARAMETER_REGEXP = Regexp.new ":\\w+"

CSV.open(outputFileName, "wb") do |out|
  out<< ["Project","File","Complex","Date", "Source","Statement","Content","isDml", "isQuery", "isParametrized","isDDL"]
  CSV.foreach(fileName, headers: true)  do |row|
    dateArray = row[FILE].scan(/Version(_\d+_\d+_)?(X_)?(\d{4})(\d{2})(\d{2})/).pop;
    dateString=dateArray[2..4].reverse.join('/')
    sql = row[SQL_STMT_COL]
    if (sql)
      ary = sql
              .split(STATEMENT_TOKEN_REGEXP)
              .drop(1)
      # next unless ary.length % 2

      ary.each_slice(2) do |token, rest|
        isDml = DML_STMT_REGEXP.match?(token)
        isQuery = SELECT_QUERY_REGEXP.match?(token)
        isParametrized=""
        isDDL = !(isDml || isQuery)
        statements = [token]

        if (isDml || isQuery)
          isParametrized =  PARAMETER_REGEXP.match? rest
        end

        if (isDml)
          statements = [token + " json values"] if (rest =~ /raw_values/)
          statements = [token + " json values"] if (rest =~ /attribute_type/)
          statements = [token + " json values"] if (rest =~ /akeneo_onboarder_message.*content/i)
        end

        if ( isDDL &&  ("ALTER".eql? token ) )
          suffixes=[]
          suffixes << "ADD COLUMN" if (rest =~ /TABLE.*ADD\s+COLUMN/m)
          suffixes << "ADD INDEX" if (rest =~ /TABLE.*ADD\s+INDEX/m)
          suffixes << "REMOVE COLUMN" if (rest =~ /TABLE.*DROP/m)
          suffixes << "RENAME COLUMN"  if (rest =~ /TABLE.*RENAME\s+AS/m)
          suffixes << "REMOVE INDEX" if (rest =~ /TABLE.*DROP\s+INDEX/m)
          suffixes << "ADD CONSTRAINT" if (rest =~ /TABLE.*ADD\s+CONSTRAINT/m)
          suffixes << "DROP INDEX" if (rest =~ /TABLE.*DROP\s+CONSTRAINT/m)
          suffixes << "ADD PRIMARY KEY" if (rest =~ /TABLE.*ADD\s+PRIMARY\s+KEY/m)
          suffixes << "DROP PRIMARY KEY" if (rest =~ /DROP\s+PRIMARY\s+KEY/m)
          suffixes << "RENAME TABLE" if (rest =~ /TABLE.*RENAME\s+AS/m)
          suffixes << "MODIFY COLUMN" if (rest =~/TABLE.*MODIFY/m)
          suffixes << "CHANGE COLUMN" if (rest =~/TABLE.*CHANGE\s+COLUMN/m)
          suffixes << "CONVERT TABLE" if (rest =~/TABLE.*CONVERT/m)
          puts suffixes
          statements = suffixes.map { |suf| "ALTER "+suf }
          puts statements
        end
        
        statements.each { |stmt|
        out << (row[0..2] << dateString << "sql" << stmt << rest <<  isDml <<  isQuery << isParametrized << isDDL)
        }

      end
    end

    es = row[ES_STMT]
    if (es)
      out << (row[0..2] << dateString << "es" << es)
    end

  end
end
