using System;
using Rochas.DapperRepository.Enums;

namespace Rochas.DapperRepository.Annotations
{
    public class DataAggregationColumn : Attribute
    {
        public string ColumnName;
        public DataAggregationType AggregationType;
    }
}
