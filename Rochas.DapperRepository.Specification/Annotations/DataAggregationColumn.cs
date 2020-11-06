using System;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Specification.Annotations
{
    public class DataAggregationColumn : Attribute
    {
        public string ColumnName;
        public DataAggregationType AggregationType;
    }
}
