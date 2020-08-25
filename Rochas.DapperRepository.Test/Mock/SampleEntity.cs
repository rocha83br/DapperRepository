﻿using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Annotations;
using Rochas.DapperRepository.Interfaces;

namespace Rochas.DapperRepository.Test
{
    [Table("SampleEntity", Schema = "dbo")]
    public class SampleEntity
    {
        [Key]
        public long DocNumber { get; set; }

        public DateTime CreationDate { get; set; }
        
        public string Name { get; set; }
        
        [Column("Person_Age")]
        public int Age { get; set; }
        
        public decimal Height { get; set; }
        
        public decimal Weight { get; set; }
        
        public bool Active { get; set; }
    }
}
