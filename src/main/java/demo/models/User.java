package demo.models;

public class User {

	private String uuid, logindata_username, logindata_password, contactdata_firstname = "n/A", contactdata_lastname = "n/A", contactdata_email = "n/A", contactdata_town = "n/A";
	
	public User(String logindata_username, String logindata_password){
		this.logindata_username = logindata_username;
		this.logindata_password = logindata_password;
	}
	
	public String getUuid(){
		return uuid;
	}
	
	public void setUuid(String uuid){
		this.uuid = uuid;
	}

	public String getLogindata_username() {
		return logindata_username;
	}

	public void setLogindata_username(String logindata_username) {
		this.logindata_username = logindata_username;
	}

	public String getLogindata_password() {
		return logindata_password;
	}

	public void setLogindata_password(String logindata_password) {
		this.logindata_password = logindata_password;
	}

	public String getContactdata_firstname() {
		return contactdata_firstname;
	}

	public void setContactdata_firstname(String contactdata_firstname) {
		this.contactdata_firstname = contactdata_firstname;
	}

	public String getContactdata_lastname() {
		return contactdata_lastname;
	}

	public void setContactdata_lastname(String contactdata_lastname) {
		this.contactdata_lastname = contactdata_lastname;
	}

	public String getContactdata_email() {
		return contactdata_email;
	}

	public void setContactdata_email(String contactdata_email) {
		this.contactdata_email = contactdata_email;
	}

	public String getContactdata_town() {
		return contactdata_town;
	}

	public void setContactdata_town(String contactdata_town) {
		this.contactdata_town = contactdata_town;
	}
	
}
