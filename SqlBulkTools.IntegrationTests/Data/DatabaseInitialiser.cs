using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests.Data
{
    public class DatabaseInitialiser : DropCreateDatabaseAlways<TestContext>
    {
        protected override void Seed(TestContext context)
        {
          
            using (var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString))
            using (var command = new SqlCommand("[dbo].[TestDataTypes]", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();
                command.CommandText = @"IF (NOT EXISTS (SELECT * 
                 FROM INFORMATION_SCHEMA.TABLES 
                 WHERE TABLE_NAME = '[dbo].[TestDataTypes]'))
                 BEGIN
                     CREATE TABLE [dbo].[TestDataTypes]
                    (
                        FloatTest float(24), 
                        FloatTest2 float,
                        DecimalTest decimal(14,2), 
                        MoneyTest money, 
                        SmallMoneyTest smallmoney,
                        NumericTest numeric(30,7),
                        RealTest real,
                        DateTimeTest datetime,
                        DateTime2Test datetime2,
                        SmallDateTimeTest smalldatetime,
                        DateTest date,
                        TimeTest time,
                        GuidTest uniqueidentifier,
                        TextTest text,
                        VarBinaryTest varbinary(20),
                        BinaryTest binary(6),
                        TinyIntTest tinyint,
                        BigIntTest bigint,
                        CharTest char(17),
                        ImageTest image,
                        NTextTest ntext,
                        NCharTest nchar(10),
                        XmlTest xml
                    );
                 END";
                command.ExecuteNonQuery();
            }
        }
    }
}
