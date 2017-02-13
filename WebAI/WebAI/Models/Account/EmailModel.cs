using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace WebAI.Models.Account
{
    public class EmailModel
    {
        public string From { get; set; }

        public string To { get; set; }

        public string Subject { get; set; }

        public string Body { get; set; }
    }
}