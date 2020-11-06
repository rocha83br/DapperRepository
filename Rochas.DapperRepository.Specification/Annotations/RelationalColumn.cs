using System;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Specification.Annotations
{
    public class RelationalColumn : Attribute
    {
        #region Declarations

        public string TableName;
        public string IntermediaryColumnName;
        public string ColumnName;
        public string ColumnAlias;
        public string KeyColumn;
        public string ForeignKeyColumn;
        public string IntermediaryColumnKey;
        public RelationalJunctionType JunctionType;
        public bool Filterable;

        public string GetColumnName()
        {
            return ColumnName;
        }

        #endregion
    }
}
