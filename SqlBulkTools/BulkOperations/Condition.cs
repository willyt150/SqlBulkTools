using System;
using System.Linq.Expressions;

// ReSharper disable once CheckNamespace
namespace SqlBulkTools
{
    /// <summary>
    /// 
    /// </summary>
    public class Condition
    {
#pragma warning disable 1591
        public string LeftName { get; set; }
        public string CustomColumnMapping { get; set; }
        public string Value { get; set; }
        public Type ValueType { get; set; }
        public ExpressionType Expression { get; set; }
    }
#pragma warning restore 1591
}
