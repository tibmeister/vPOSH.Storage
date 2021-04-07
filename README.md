![GitHub issues](https://img.shields.io/github/issues-raw/tibmeister/vPOSH.Storage?style=plastic) ![GitHub](https://img.shields.io/github/license/tibmeister/vPOSH.Storage?style=plastic)

# vPOSH.Storage

PowerShell based framework for VMware vSphere. As a framework, there will be a number of external modules that will be part of this framework, but the core of the framework is in this repository.

# Prerequisites

Powershell v5 or higher must be installed along with PowerCLI 10.2.0.9372002 or higher.

# Installing / Getting Started

A Windows Environment Variable is required in order to get things working correctly.  The variable **$env:HOME** needs to be set, and the easiest way to do this is as follows:

```
[System.Environment]::SetEnvironmentVariable('HOME','c:\Users\{USERNAME}',[System.EnvironmentVariableTarget]::User)
```
Change **{USERNAME}** to your username.  Windows variables like %USERNAME% do not work in Powershell, so setting this at the user level to your username should be fine.

A **vcenters.json** file must be present in the *$env:HOME\vPOSH\.config* folder in order to allow for a Connect-vCenter cmdlet to function correctly. the format of this file is as follows:

```
[
    {
        "vCenter": "vCenterFQDN",
        "AutoConnect": true,
        "Environment": "Production",
        "Location": "Datacenter1"
    }
]
```

For **AutoConnect**, this is a true/false value and will be used to determine if the **AutoConnect** feature of the cmdlet is used.  This is slated for a future release

# Versioning

Based on Semantec Versioning, the following will be used:
*Major Version.Minor Version.Patch*

Any new module will trigger a Minor version change, as will any new feature being added. Removing a feature or any other major changes that could break any existing code will trigger a Major version change. General commits to fix issues or enhance existing features will trigger a Patch increment.

# Special Notes

* The *.config* directory is not included in the repository and requires creation.
* The *Modules* directory will only have the modules for this framework.  Adding other modules will have no effect on the repository.

# Release History