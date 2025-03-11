public class UserRequestModel
{
    public string SocialID { get; set; }
    public string SocialType { get; set; } // 'Google' or 'Facebook'
    public string Email { get; set; }
    public string FirstName { get; set; }
    public string LastName { get; set; }
    public string Username { get; set; }
    public string PhoneNumber { get; set; } // Optional
    public DateTime? DateOfBirth { get; set; } // Optional
    public string Gender { get; set; } // Optional
    public string Country { get; set; } // Optional
    public string LanguagePreference { get; set; } // Optional
    public string ThemePreference { get; set; } // Optional
    public string NotificationPreferences { get; set; } // Optional
}
