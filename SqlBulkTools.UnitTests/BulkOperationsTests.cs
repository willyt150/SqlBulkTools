using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq.Expressions;
using Moq;
using NUnit.Framework;
using SqlBulkTools.IntegrationTests;
using SqlBulkTools.IntegrationTests.Model;
using SqlBulkTools.IntegrationTests.TestEnvironment;
using SqlBulkTools.UnitTests.Model;

namespace SqlBulkTools.UnitTests
{
    [TestFixture]
    class BulkOperationsTests
    {

        [Test]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithThreeConditions()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId", "AddressId" };
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

            // Assert
            Assert.AreEqual("ON [Target].[MarketPlaceId] = [Source].[MarketPlaceId] AND [Target].[FK_BusinessId] = [Source].[FK_BusinessId] AND [Target].[AddressId] = [Source].[AddressId] ", result);
        }

        [Test]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWithTwoConditions()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId", "FK_BusinessId" };
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

            // Assert
            Assert.AreEqual("ON [Target].[MarketPlaceId] = [Source].[MarketPlaceId] AND [Target].[FK_BusinessId] = [Source].[FK_BusinessId] ", result);
        }

        [Test]
        public void BulkOperationsHelpers_BuildJoinConditionsForUpdateOrInsertWitSingleCondition()
        {
            // Arrange
            List<string> joinOnList = new List<string>() { "MarketPlaceId" };
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildJoinConditionsForUpdateOrInsert(joinOnList.ToArray(), "Source", "Target");

            // Assert
            Assert.AreEqual("ON [Target].[MarketPlaceId] = [Source].[MarketPlaceId] ", result);
        }

        [Test]
        public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestColumns();
            var expected =
                "SET [Target].[id] = [Source].[id], [Target].[Name] = [Source].[Name], [Target].[Town] = [Source].[Town], [Target].[Email] = [Source].[Email], [Target].[IsCool] = [Source].[IsCool] ";
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_BuildUpdateSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");

            var expected =
                "SET [Target].[Id] = [Source].[Id] ";
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildUpdateSet(updateOrInsertColumns, "Source", "Target", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_BuildInsertSet_BuildsCorrectSequenceForMultipleColumns()
        {
            // Arrange
            var updateOrInsertColumns = GetTestColumns();
            var expected =
                "INSERT ([Name], [Town], [Email], [IsCool]) values ([Source].[Name], [Source].[Town], [Source].[Email], [Source].[IsCool])";
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildInsertSet(updateOrInsertColumns, "Source", "id");

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_BuildInsertIntoSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var columns = new HashSet<string>();
            columns.Add("Id");
            var tableName = "TableName";

            var expected = "INSERT INTO TableName ([Id]) ";
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildInsertIntoSet(columns, null, tableName);

            // Assert
            Assert.AreEqual(result, expected);
        }

        [Test]
        public void BulkOperationsHelpers_BuildInsertIntoSet_BuildsCorrectSequenceForMultipleColumns()
        {
            var columns = GetTestColumns();
            var tableName = "TableName";
            var expected =
                "INSERT INTO TableName ([Name], [Town], [Email], [IsCool]) ";

            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildInsertIntoSet(columns, "id", tableName);

            // Assert
            Assert.AreEqual(result, expected);

        }

        [Test]
        public void BulkOperationsHelpers_BuildInsertSet_BuildsCorrectSequenceForSingleColumn()
        {
            // Arrange
            var updateOrInsertColumns = new HashSet<string>();
            updateOrInsertColumns.Add("Id");
            var expected =
                "INSERT ([Id]) values ([Source].[Id])";
            var sut = new BulkOperationsHelper();

            // Act
            var result = sut.BuildInsertSet(updateOrInsertColumns, "Source", null);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_GetAllValueTypeAndStringColumns_ReturnsCorrectSet()
        {
            // Arrange
            BulkOperationsHelper helper = new BulkOperationsHelper();
            HashSet<string> expected = new HashSet<string>() {"Title", "CreatedTime", "BoolTest", "IntegerTest", "Price"};

            // Act
            var result = helper.GetAllValueTypeAndStringColumns(typeof (ModelWithMixedTypes));

            // Assert
            CollectionAssert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelpers_GetIndexManagementCmd_WhenDisableAllIndexesIsTrueReturnsCorrectCmd()
        {
            // Arrange
            string expected =
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;' FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = '[SqlBulkTools].[dbo].[Books]'; EXEC(@sql);";
            BulkOperationsHelper helper = new BulkOperationsHelper();
            var databaseName = "SqlBulkTools";

            var sqlConnMock = new Mock<IDbConnection>();
            sqlConnMock.Setup(x => x.Database).Returns(databaseName);

            // Act
            string result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", "dbo", sqlConnMock.Object, null, true);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_GetIndexManagementCmd_WithOneIndexReturnsCorrectCmd()
        {
            // Arrange
            string expected =
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;' FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = '[SqlBulkTools].[dbo].[Books]' AND sys.indexes.name = 'IX_Title'; EXEC(@sql);";
            BulkOperationsHelper helper = new BulkOperationsHelper();
            HashSet<string> indexes = new HashSet<string>();
            indexes.Add("IX_Title");
            var databaseName = "SqlBulkTools";

            var sqlConnMock = new Mock<IDbConnection>();
            sqlConnMock.Setup(x => x.Database).Returns(databaseName);

            // Act
            string result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", "dbo", sqlConnMock.Object, indexes);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_RebuildSchema_WithExplicitSchemaIsCorrect()
        {
            // Arrange
            string expected = "[db].[CustomSchemaName].[TableName]";
            BulkOperationsHelper helper = new BulkOperationsHelper();

            // Act
            string result = helper.GetFullQualifyingTableName("db", "CustomSchemaName", "TableName");

            // Act
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelpers_GetIndexManagementCmd_WithListOfIndexesReturnsCorrectCmd()
        {
            // Arrange
            string expected =
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;' FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = '[SqlBulkTools].[dbo].[Books]' AND sys.indexes.name = 'IX_Title' AND sys.indexes.name = 'IX_Price'; EXEC(@sql);";
            BulkOperationsHelper helper = new BulkOperationsHelper();
            HashSet<string> indexes = new HashSet<string>();
            indexes.Add("IX_Title");
            indexes.Add("IX_Price");

            var databaseName = "SqlBulkTools";

            var sqlConnMock = new Mock<IDbConnection>();
            sqlConnMock.Setup(x => x.Database).Returns(databaseName);

            // Act
            string result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", "dbo", sqlConnMock.Object, indexes);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelper_GetDropTmpTableCmd_ReturnsCorrectCmd()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var expected = "DROP TABLE #TmpOutput;";

            // Act
            var result = helper.GetDropTmpTableCmd();

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThanDecimalCondition()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] {"stub"};
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.LessThan,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof (decimal)
                }
            };

            var expected = "AND [Target].[Price] < @Price ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_IsNullCondition()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.Equal,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] IS NULL ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_IsNotNullCondition()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] IS NOT NULL ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThan()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.LessThan,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] < @Description ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_LessThanOrEqualTo()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.LessThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] <= @Description ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_GreaterThan()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.GreaterThan,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] > @Description ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_GreaterThanOrEqualTo()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                }
            };

            var expected = "AND [Target].[Description] >= @Description ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_CustomColumnMapping()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Description",
                    Value = "null",
                    CustomColumnMapping = "ShortDescription"
                }
            };

            var expected = "AND [Target].[ShortDescription] >= @Description ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_MultipleConditions()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn = new[] { "stub" };
            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                },
                new Condition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof(decimal)
                },
            };

            var expected = "AND [Target].[Description] IS NOT NULL AND [Target].[Price] >= @Price ";

            // Act
            var result = helper.BuildPredicateQuery(updateOn, conditions, targetAlias);

            // Assert
            Assert.AreEqual(expected, result);
        }

        [Test]
        public void BulkOperationsHelper_BuildPredicateQuery_ThrowsWhenUpdateOnColIsEmpty()
        {
            // Arrange
            var helper = new BulkOperationsHelper();
            var targetAlias = "Target";
            var updateOn1 = new string[0];

            var conditions = new List<Condition>()
            {
                new Condition()
                {
                    Expression = ExpressionType.NotEqual,
                    LeftName = "Description",
                    Value = "null",
                },
                new Condition()
                {
                    Expression = ExpressionType.GreaterThanOrEqual,
                    LeftName = "Price",
                    Value = "50",
                    ValueType = typeof(decimal)
                },
            };

            Assert.Throws<SqlBulkToolsException>(() => helper.BuildPredicateQuery(updateOn1, conditions, targetAlias));
            Assert.Throws<SqlBulkToolsException>(() => helper.BuildPredicateQuery(null, conditions, targetAlias));
        }

        [Test]
        public void BulkOperationsHelper_AddSqlParamsForUpdateQuery_GetsTypeAndValue()
        {
            var helper = new BulkOperationsHelper();

            Book book = new Book()
            {
                ISBN = "Some ISBN",
                Price = 23.99M,
                BestSeller = true
            };

            HashSet<string> columns = new HashSet<string>();
            columns.Add("ISBN");
            columns.Add("Price");
            columns.Add("BestSeller");

            List<SqlParameter> sqlParams = new List<SqlParameter>();

            helper.AddSqlParamsForUpdateQuery(sqlParams, columns, book);

            Assert.AreEqual(3, sqlParams.Count);
        }

        private HashSet<string> GetTestColumns()
        {
            HashSet<string> parameters = new HashSet<string>();

            parameters.Add("id");
            parameters.Add("Name");
            parameters.Add("Town");
            parameters.Add("Email");
            parameters.Add("IsCool");

            return parameters;
        }

        private HashSet<string> GetBookColumns()
        {
            HashSet<string> parameters = new HashSet<string>();

            parameters.Add("Id");
            parameters.Add("ISBN");
            parameters.Add("Title");            
            parameters.Add("PublishDate");
            parameters.Add("Price");

            return parameters;
        } 
    }
}
