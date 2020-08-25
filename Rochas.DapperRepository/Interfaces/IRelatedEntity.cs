using System;
using System.Collections.Generic;
using Rochas.DapperRepository.Enums;

namespace Rochas.DapperRepository.Interfaces
{
    interface IRelatedEntity
    {
        RelationCardinality GetRelationCardinality();
        Type GetIntermediaryEntity();
        string GetIntermediaryKeyAttribute();
    }
}
