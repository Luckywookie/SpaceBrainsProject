using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Web;

namespace WebAI.Models.Account
{
    public class UserViewModel
        : Base.BaseModel
    {
        [DisplayName("Логин")]
        public string Login { get; set; }
        
        [DisplayName("Email")]
        //[DataType(DataType.EmailAddress)]
        public string Email { get; set; }

        [DisplayName("Имя")]
        public string Name { get; set; }
    }
}