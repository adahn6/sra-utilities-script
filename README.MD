This script is designed to replicate the failover XML file creation from SRDF Adapter Utilities for vCenter. While there is other functionality offered by SRDF Adapter Utilities, this script only address the failover pairing file creation. Additionally, 
note that this is not designed to be as robust as the vCenter plugin. Instead, this script targets the most common use cases (two site, one local SMI-S array, etc). While we are unlikely to support new additions to the script, feel free to file an issue
 if you come across any bugs-- we'll try to respond as we can. For more info, check out [Drew Tonnesen's blog post on the script](https://drewtonnesen.wordpress.com/2017/03/17/srdf-sra-srdf-au-and-vcsa/).

## Pre-requisites:

* Python 2.7 with packages pywbem, pyVmomi, and lxml.
* Both the R2 devices and the target devices (SnapVX targets) must be presented to the recovery site. As I mentioned we can’t do automated masking. If the device is not seen by your hosts in the vCenter, the script will not find them.
* An SMI-S environment listening on a non-SSL port. By default, SMI-S 8.3 listens only on SSL 5989. If you need help modifying SMI-S to listen on non-SSL see [this post](https://drewtonnesen.wordpress.com/2016/06/03/vsi-6-9-smi-s-6-0u2/).
* You will have to modify the script to put in the variables for the Recovery vCenter, SMI-S, copy type and the file name. Here is the top portion of the script that requires updating:

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
```


## Notes:

* Only designed to be used in a 2-site configuration. Has not been tested with SRDF/Metro or any 3-site solutions.
* The SMI-S environment used must only see the recovery array as local, otherwise the script will fail.
* If the SMI-S environment has not been updated within the last 15 minutes, the script will fail. SMI-S will refresh every hour automatically, but if you need to refresh manually, execute TestSMIProvider and use the refsys command. The reason we did this is to ensure if you make last minute changes to the configuration (e.g. add pairs), SMI-S knows about it. If you have a static environment, you can easily change the time in the script to something longer so it doesn’t prevent you from running it.
* The script was tested with SRDF SRA 6.2, SRM 6.1, SMI-S 8.3, and vCenter 6.0.
* We use SnapVX/NOCOPY as the replication type/mode. If you have to use emulation mode, you can adjust the script so the XML file that is produced is correct.
* Though the XML file is created automatically, you will still have to copy it to the appropriate directory on your SRM recovery site.
* The script will generate pairs for every device in your SRM environment. The nice thing about the SRDF SRA is that it doesn’t care if you have extra pairs in the Test Failover XML file. It only looks for the pairs it needs for the recovery plan. Now there is one drawback to this. If you have lots of R2 devices at the recovery site that are not used with SRM, the script is going to fail unless you have device targets presented to the vCenter for those devices. There are ways to get around this with a little thought but we leave that in your capable hands.
