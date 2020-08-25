﻿using Rochas.DapperRepository.Enums;
using System;
using System.Collections.Generic;
using System.Text;

namespace Rochas.DapperRepository.Annotations
{
    public class FilterableAttribute : Attribute
    {
        FilterBehavior FilterBehavior { get; set; }
        string ComparationProperty { get; set; }
    }
}
