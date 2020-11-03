using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using Rochas.DapperRepository.Annotations;
using Rochas.DapperRepository.Interfaces;

namespace Rochas.DapperRepository.Test
{
    [Table("sample_entity")]
    public class SampleEntity
    {
        [Key]
        [Column("doc_number")]
        public long DocNumber { get; set; }

        [Column("creation_date")]
        public DateTime CreationDate { get; set; }
        
        [Filterable]
        [Column("name")]
        public string Name { get; set; }

        [Column("age")]
        public int Age { get; set; }

        [Column("height")]
        public decimal Height { get; set; }

        [Column("weight")]
        public decimal Weight { get; set; }

        [Column("active")]
        public bool Active { get; set; }
    }
}
