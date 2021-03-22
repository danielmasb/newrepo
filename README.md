# DataGrip
## What is DataGrip?

DataGrip is a database management environment for developers. It supports MySQL, PostgreSQL, Snowflake, and many more. 
You can query all your databases in one only tool.

## Setup

1. You will receive a Returnly license via email in your first days
1. Download DataGrip from https://www.jetbrains.com/datagrip/download/#section=mac
1. Follow a regular installation


## Returnly formatting settings

We want to make sure that we are all using the same formatting settings in DataGrip.
For this work, please follow the following steps:

1. Go to DataGrip -> Preferences -> Plugins, and install the Settings Repository plugin
1. Restart DataGrip
1. Go to File -> Manage IDE Settings -> Settings Repository
1. Enter `git@github.com:returnly/datagrip-settings.git` and pick overwrite local

#### Troubleshooting
If you have any problem connecting to GitHub from DataGrip you can install the *datagrip-settings* repo from local:

1. Clone the repository from https://github.com/returnly/datagrip-settings
    ```
    git clone https://github.com/returnly/datagrip-settings
    ```
1. In DataGrip, go to File -> Manage IDE Settings -> Settings Repository
1. Enter the repo local path and pick overwrite local


## How to use DataGrip?
Read the following [internal guide](https://github.com/returnly/internal-wiki/blob/master/workstation/data_grip.md)
