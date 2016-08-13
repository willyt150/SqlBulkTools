using System;
using System.Collections.Generic;
using System.Data;
using NUnit.Framework;
using SqlBulkTools.IntegrationTests.Model;
using SqlBulkTools.UnitTests.Model;

namespace SqlBulkTools.UnitTests
{
    [TestFixture]
    class SqlBulkToolsUnitTests
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
                "UPDATE SET [Target].[id] = [Source].[id], [Target].[Name] = [Source].[Name], [Target].[Town] = [Source].[Town], [Target].[Email] = [Source].[Email], [Target].[IsCool] = [Source].[IsCool] ";
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
                "UPDATE SET [Target].[Id] = [Source].[Id] ";
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
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;'FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = 'Books'; EXEC(@sql);";
            BulkOperationsHelper helper = new BulkOperationsHelper();

            // Act
            string result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", null, true);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void BulkOperationsHelpers_GetIndexManagementCmd_WithOneIndexReturnsCorrectCmd()
        {
            // Arrange
            string expected =
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;'FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = 'Books' AND sys.indexes.name = 'IX_Title'; EXEC(@sql);";
            BulkOperationsHelper helper = new BulkOperationsHelper();
            HashSet<string> indexes = new HashSet<string>();
            indexes.Add("IX_Title");

            // Act
            string result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", indexes);

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
                @"DECLARE @sql AS VARCHAR(MAX)=''; SELECT @sql = @sql + 'ALTER INDEX ' + sys.indexes.name + ' ON ' + sys.objects.name + ' DISABLE;'FROM sys.indexes JOIN sys.objects ON sys.indexes.object_id = sys.objects.object_id WHERE sys.indexes.type_desc = 'NONCLUSTERED' AND sys.objects.type_desc = 'USER_TABLE' AND sys.objects.name = 'Books' AND sys.indexes.name = 'IX_Title' AND sys.indexes.name = 'IX_Price'; EXEC(@sql);";
            BulkOperationsHelper helper = new BulkOperationsHelper();
            HashSet<string> indexes = new HashSet<string>();
            indexes.Add("IX_Title");
            indexes.Add("IX_Price");

            // Act
            string result = helper.GetIndexManagementCmd(IndexOperation.Disable, "Books", indexes);

            // Assert
            Assert.AreEqual(expected, result);

        }

        [Test]
        public void DataTableTools_GetColumn_RetrievesColumn()
        {
            // Arrange
            DataTableTools<Book> dtTools = new DataTableTools<Book>(null, GetBookColumns(), null);
            var expected1 = "ISBN";
            var expected2 = "Price";    

            // Act
            var result1 = dtTools.GetColumn(x => x.ISBN);
            var result2 = dtTools.GetColumn(x => x.Price);

            // Assert
            Assert.AreEqual(expected1, result1);
            Assert.AreEqual(expected2, result2);
        }

        [Test]
        public void DataTableTools_GetColumn_ThrowExceptionWhenColumnNotFound()
        {
            // Arrange
            DataTableTools<Book> dtTools = new DataTableTools<Book>(null, GetBookColumns(), null);

            // Act and Assert
            Assert.Throws<InvalidOperationException>(() => dtTools.GetColumn(x => x.Description));
        }

        [Test]
        public void DataTableTools_GetColumn_ThrowExceptionWhenColumnsIsNull()
        {
            // Arrange
            DataTableTools<Book> dtTools = new DataTableTools<Book>(null, null, null);

            // Act and Assert
            Assert.Throws<NullReferenceException>(() => dtTools.GetColumn(x => x.ISBN));

        }

        [Test]
        public void DataTableTools_GetColumn_ThrowExceptionWhenColumnMappingNotFound()
        {
            // Arrange
            DataTableTools<Book> dtTools = new DataTableTools<Book>(null, GetBookColumns(), null);

            Assert.Throws<InvalidOperationException>(() => dtTools.GetColumn(x => x.Description));
        }

        [Test]
        public void DataTableTools_GetColumn_CustomColumnMapsCorrectly()
        {
            // Arrange
            Dictionary<string, string> CustomColumns = new Dictionary<string, string>()
            { {"PublishDate", "PublishingDate"} };

            DataTableTools<Book> dtTools = new DataTableTools<Book>(null, GetBookColumns(), CustomColumns);
            var expected = "PublishingDate";


            // Act
            var result = dtTools.GetColumn(x => x.PublishDate);

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
