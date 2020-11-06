using System;
using Rochas.DapperRepository.Specification.Enums;

namespace Rochas.DapperRepository.Specification.Interfaces
{
    interface IRelatedEntity
    {
        RelationCardinality GetRelationCardinality();
        Type GetIntermediaryEntity();
        string GetIntermediaryKeyAttribute();
    }
}
