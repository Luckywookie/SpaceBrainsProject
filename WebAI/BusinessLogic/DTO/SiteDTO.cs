﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessLogic.DTO
{
    public class SiteDTO
        : Base.BaseDTO
    {
        public string Name { get; set; }
        public string Url { get; set; }

    }
}
