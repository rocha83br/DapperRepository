using System;
using Rochas.DapperRepository.Specification.Enums;
using Rochas.DapperRepository.Specification.Interfaces;

namespace Rochas.DapperRepository.Specification.Annotations
{
    public class RelatedEntity : Attribute, IRelatedEntity
    {
        public RelationCardinality Cardinality;
        public string ForeignKeyAttribute;
        public Type IntermediaryEntity;
        public string IntermediaryKeyAttribute;
        public bool RecordableComposition = false;

        public RelationCardinality GetRelationCardinality()
        {
            return Cardinality;
        }

        public Type GetIntermediaryEntity()
        {
            return IntermediaryEntity;
        }

        public string GetIntermediaryKeyAttribute()
        {
            return IntermediaryKeyAttribute;
        }
    }
}
