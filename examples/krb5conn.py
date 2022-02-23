import stardog
from requests_kerberos import HTTPKerberosAuth


def main():
    connection_details = {
        "endpoint": "http://localhost:5820",
        "auth": HTTPKerberosAuth(),
    }
    with stardog.Admin(**connection_details) as admin:
        users = admin.users()
        for u in users:
            print(u.name)


if __name__ == "__main__":
    # execute only if run as a script
    main()
