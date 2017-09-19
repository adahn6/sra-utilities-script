This script is designed to replicate certain utilities from the discontinued SRDF Adapter Utilities plugin. Specifically, this script only address the failover pairing file creation and the masking info XML file creation. Additionally, 
note that this is not designed to be as robust as the vCenter plugin. Instead, this script targets the most common use cases (two site, one local SMI-S array, etc). While we are unlikely to support new additions to the script, feel free to file an issue
 if you come across any bugs-- we'll try to respond as we can. For more info, check out [Drew Tonnesen's blog post on the failover script functionality](https://drewtonnesen.wordpress.com/2017/03/17/srdf-sra-srdf-au-and-vcsa/) and [on the masking info script functionality](https://drewtonnesen.wordpress.com/2017/09/19/srdf-sra-python-masking/).

## Usage:

### Basic help output
```
srau.py [-h] [--debug] [--usage {failover,maskinginfo}]

SRDF Utilies script

optional arguments:
  -h, --help            show this help message and exit
  --debug               enable debug logging
  --usage {failover,maskinginfo}
                        create failover XML file or create masking info and
                        failover XML files
```

The usage parameter is the primary parameter; it is used to determine what utility the script will provide. For example, to create a failover XML file, the script should be run as: ```python srau.py --usage=failover```
while to create failover and masking info XML files, the script should be run as ```python srau.py --usage=maskinginfo```

## Environment details:
* Python 2.7 with packages pywbem, pyVmomi, and lxml.
* An SMI-S environment, preferably listening on a non-SSL port. By default, SMI-S 8.3 listens only on SSL 5989. If you need help modifying SMI-S to listen on non-SSL see [this post](https://drewtonnesen.wordpress.com/2016/06/03/vsi-6-9-smi-s-6-0u2/).
* A vCenter environment

Update this [section](https://github.com/adahn6/sra-utilities-script/blob/master/srau.py#L16) of the script with these details:
```
SMIS_IP = "http://192.168.1.1"
SMIS_PORT = "5988"
SMIS_USER = "admin"
SMIS_PASS = "#1Password"

VCENTER_IP = "192.168.1.2"
VCENTER_PORT = "443"
VCENTER_USER = "administrator@vsphere.local"
VCENTER_PASS = "password"

COPY_TYPE = "SNAPVX"
COPY_MODE = "NOCOPY"

FILENAME = "EmcSrdfSraTestFailoverConfig.xml"
FILENAME_MASKING_INFO = "EmcSrdfSraMaskingInfo.xml"
FILENAME_ALL_DEVICES = "AllMaskingDevices.txt"
```

## Requirements and notes:

## General:
* Only designed to be used in a 2-site configuration. Has not been tested with SRDF/Metro or any 3-site solutions.
* The SMI-S environment used must only see the recovery array as local. The script will check for this and fail out if this requirement isn't met.
* The SMI-S environment must have have been updated within the last 15 minutes. The script will check for this and fail out if this requirement isn't met. SMI-S will refresh every hour automatically, but if you need to refresh manually, execute TestSMIProvider and use the refsys command. The reason we did this is to ensure if you make last minute changes to the configuration (e.g. add pairs), the SMI-S data will reflect those changes. If you have a static environment, you can easily change the time in the script to something longer so it doesn’t prevent you from running it.
* The script was tested with SRDF SRA 6.2, SRM 6.1, SMI-S 8.3, and vCenter 6.0.
* We use SnapVX/NOCOPY as the replication type/mode. If you have to use emulation mode, you can adjust the script so the XML file that is produced is correct.
* Though the XML file is created automatically, you will still have to copy it to the appropriate directory on your SRM recovery site.
* The script will generate pairs for every device in your SRM environment. The nice thing about the SRDF SRA is that it doesn’t care if you have extra pairs in the Test Failover XML file. It only looks for the pairs it needs for the recovery plan. Now there is one drawback to this. If you have lots of R2 devices at the recovery site that are not used with SRM, the script is going to fail unless you have device targets presented to the vCenter for those devices. There are ways to get around this with a little thought but we leave that in your capable hands.


### Failover functionality :
* Both the R2 devices and the target devices (SnapVX targets) must be presented to the recovery site. As I mentioned we can’t do automated masking. If the device is not seen by your hosts in the vCenter, the script will not find them.

### Masking info functionality requirements:
* When using the masking capability, the script will not include any TDEV targets that are in replication relationships. For more info, consult [Drew's blog post](https://drewtonnesen.wordpress.com/2017/09/19/srdf-sra-python-masking/)
