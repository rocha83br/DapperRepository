using Rochas.DapperRepository.Enums;
using Rochas.DapperRepository.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.DapperRepository.Annotations
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
