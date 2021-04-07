<#
    .SYNOPSIS
        Module to manag4 various storage functions within vSphere.  May require PowerCLI
#>
function Get-NFSVolumeHostInfo
{
	<#
		.SYNOPSIS
			[PowerCLI] Display the location (UUID) of a datastore on the given host, or all datastores on the given host
		.DESCRIPTION
			Display the location (UUID) of a datastore on the given host, or all datastores on the given host
		.PARAMETER VMHost
			Name of the VMHost to look on
		.PARAMETER Datastore
			Name of Datastore to find
	#>

	[CmdletBinding()]
	param
	(
		[Parameter(Mandatory=$true)]
		[string]$VMHost,

		[Parameter(Mandatory=$false)]
		[String[]]$Datastores
	)

	$ds = get-view (get-view (Get-VMHost $VMHost).ID).ConfigManager.StorageSystem
	if($Datastores)
	{
		$temp = ($ds.FileSystemVolumeInfo.MountInfo | Where {$_.Volume.Type -match "NFS" -and $Datastores -ccontains $_.Volume.Name} | Select `
	    	@{N="Name";E={($_ | Select -ExpandProperty Volume | Select Name).Name}}, `
	    	@{N="RemoteHost";E={($_ | Select -ExpandProperty Volume | Select RemoteHost).RemoteHost}}, `
	    	@{N="RemotePath";E={($_ | Select -ExpandProperty Volume | Select RemotePath).RemotePath}}, `
	    	@{N="LocalPath";E={($_ | Select -ExpandProperty MountInfo | Select Path).Path -replace "/vmfs/volumes/",""}})
	}
	else
	{
		$temp = ($ds.FileSystemVolumeInfo.MountInfo | Where {$_.Volume.Type -match "NFS"} | Select `
	    	@{N="Name";E={($_ | Select -ExpandProperty Volume | Select Name).Name}}, `
	    	@{N="RemoteHost";E={($_ | Select -ExpandProperty Volume | Select RemoteHost).RemoteHost}}, `
	    	@{N="RemotePath";E={($_ | Select -ExpandProperty Volume | Select RemotePath).RemotePath}}, `
	    	@{N="LocalPath";E={($_ | Select -ExpandProperty MountInfo | Select Path).Path -replace "/vmfs/volumes/",""}})
	}

	return $temp
}


function Get-DoubleMountedNFS
{
	<#
		.SYNOPSIS
			[PowerCLI] Get's all the NFS Datastores that are mounted to more than one cluster
		.DESCRIPTION
			Get's all the NFS Datastores that are mounted to more than one cluster and returns the name and the clusters it is mounted to.  This runs on all connected vCenters unless you specify a Datacenter
		.PARAMETER Datacenter
			Name of Datacenter you want to restrict your search to
	#>
	param
	(
		[Parameter(Mandatory=$false,
		HelpMessage="Get's all the NFS Datastores that are mounted to more than one cluster")]
		[string]$Datacenter
	)
	$ht=@{}
	if($Datacenter)
	{
		$myArray= Get-Datacenter $Datacenter | get-datastore | Where {$_.Type -match "NFS" -and $_.Name -notmatch "swp" -and $_.Name -notmatch "guestOsMedia"} | Select Name, Datacenter | sort Name
	}
	else
	{
		$myArray=get-datastore | Where {$_.Type -match "NFS" -and $_.Name -notmatch "swp" -and $_.Name -notmatch "guestOsMedia"} | Select Name, Datacenter | sort Name
	}

	$myArray | %{$ht[$_.Name] += 1}
	$ht.Keys | Where {$ht["$_"] -gt 1} | % {write-host "Datastore $_ is mounted to more than one cluster"}
	$matches = $ht.Keys | Where {$ht["$_"] -gt 1}

	$outObj = foreach($obj in ($myArray | Where {$matches -contains $_.Name} | group -Property Name))
	{
		$tmpObj = ""
		foreach($item in $obj.Group)
		{
			if($tmpObj.Length -gt 0)
			{
				$tmpObj = "$($tmpObj),$($item.Datacenter)"
			}
			else
			{
				$tmpObj = $item.Datacenter
			}
		}
		New-Object PSObject -Property @{
			Datastore = $obj.Name
			Datacenters = $tmpObj
		}
	}

	return $outObj
}

function Start-SequentialSvMotion
{
	<#
	.SYNOPSIS
	    [PowerCLI]Perform sequential Storage vMotion on a list of VMs
	.DESCRIPTION
	    Perform sequential Storage vMotion on a list of VMs
	.PARAMETER VMs
	   	List of VM's to move
	.PARAMETER DestinationDatastore
	   	Name of the datastore to move the VM(s) to
	.PARAMETER IsCluster
			Is the DestinationDatastore a Storage DRS Cluster
	.PARAMETER NumOfSeqTasks
			Amount of simultanious Storage vMotions to perform sequentially
	.PARAMETER NumSecWait
			Number of seconds to wait between checks, minimum of 10 seconds
	.EXAMPLE
			Start-SequentialSVMotion.ps1 -VMs "myVM1","MyVM2" -DestinationDatastore datastore
	.EXAMPLE
			Start-SequentialSvMotion -VMs (Get-Cluster MyCluster | Get-VM) -DestinationDatastore MyDatastore
	.EXAMPLE
			Start-SequentialSvMotion -VMs "myVM" -DestinationDatastore MyDatastoreCluster -IsCluster:$True
	.EXAMPLE
			Start-SequentialSvMotion -VMs "myVM" -DestinationDatastore MyDatastore -NumOfSeqTasks 5
	#>

	param (
		[Parameter(Mandatory=$True,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="List of VM's to move")]
		[string[]]$VMs,

		[Parameter(Mandatory=$True,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Name of the datastore to move the VM(s) to")]
		[VMware.VimAutomation.ViCore.Impl.V1.DatastoreManagement.DatastoreImpl]$DestinationDatastore,

		[Parameter(Mandatory=$false,
		HelpMessage="Is the DestinationDatastore a Storage DRS Cluster")]
		[switch]$IsCluster=$false,

		[Parameter(Mandatory=$false,
		HelpMessage="Amount of simultanious Storage vMotions to perform sequentially")]
		[int]$NumOfSeqTasks=2,

		[Parameter(Mandatory=$false,
		HelpMessage="Number of seconds to wait between checks, minimum of 10 seconds")]
		[int]$NumSecWait=60
	)

	[switch]$thinProvision=$false

	if($NumSecWait -lt 10)
	{
		$NumSecWait = 10
	}

	clear-host

	if($IsCluster)
	{
		if((get-datastorecluster $DestinationDatastore | get-datastore | select -first 1).Type -eq "NFS")
		{
			$thinProvision=$true
		}
	}
	else
	{
		if((get-datastore $DestinationDatastore).Type -eq "NFS")
		{
			$thinProvision=$true
		}
	}

	$jobs = New-Object -typeName System.Collections.Arraylist
	$sFormat = "Thick"

	if($thinProvision)
	{
		$sFormat = "Thin"
	}

	foreach ( $vm in $VMs)
	{
		$runCount = (get-task | Where {$_.Name -eq "RelocateVM_Task" -or $_.name -eq "ApplyStorageDrsRecommendation_Task"} | Where State -eq "Running").Count

		while($runCount -ge $NumOfSeqTasks)
		{
			write-host "$($runCount) of $($NumOfSeqTasks) Active Jobs, limit reached.  Waiting..."
			sleep $NumSecWait
			$runCount = (get-task | Where {$_.Name -eq "RelocateVM_Task" -or $_.name -eq "ApplyStorageDrsRecommendation_Task"} | Where State -eq "Running").Count
		}

		if($runCount -lt $NumOfSeqTasks)
		{
			write-host "Starting relocation of $($vm)"
			$j=Get-vm $vm | Move-VM -Datastore $DestinationDatastore -DiskStorageFormat $sFormat -Confirm:$false -RunAsync -ErrorAction SilentlyContinue
			$jobs.add($j)
		}

		sleep $NumSecWait
	}

	$resultObj=foreach($job in $jobs)
	{
		$job = get-task | where Id -eq $job.Id
		if($job.State -ne "Running")
		{
			#$jobs.remove($job)
			New-Object PSObject -Property @{
				VMName=$vm
				Result=$job.State
				StartTime=$job.StartTime
				EndTime=$job.EndTime
			}
		}
	}
	return $resultObj
}

function Remove-DataStoreFromDataCenter
{
	<#
	.SYNOPSIS
		[PowerCLI]Unmounts a specified Datastore from an entire Datacenter
	.DESCRIPTION
		Unmounts a specified Datastore from an entire Datacenter
	.EXAMPLE
		Remove-DataStoreFromDataCenter -DataCenterName "dc1" -DatastoreName ds1
	.PARAMETER DataCenterName
		The name of the Datacenter you want to work with
	.PARAMETER DatastoreName
		Name of the Datastore you want to unmount
	#>
	param
	(
		[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Name of the Datastore you want to unmount")]
		[string]$DatastoreName,

		[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="The name of the Datacenter you want to work with")]
		[string]$DataCenterName
	)

	foreach ($vmHost in (Get-Datacenter $DataCenterName | Get-VMHost))
	{
		$dataStore = $vmhost | Get-Datastore $DatastoreName -ErrorAction SilentlyContinue
		if($dataStore)
		{
			$dataStore | Remove-Datastore -VMHost $vmhost -Confirm:$false -ErrorAction SilentlyContinue
		}
	}
}

function Get-VMperNFSDatastore
{
	<#
	.SYSNOPSIS
		[PowerCLI]Retrieves a count of the number of VMs on a Datastore
	.DESCRIPTION
		Retrieves a count of the number of VMs on a Datastore in an ordered list sutable for parsing our CSV output.  This
		runs against the currently connected vCenter(s) or just a single DataCenter (Must be connected first) and will only run against NFS volumes for VM's
	.EXAMPLE
		Get-VMperNFSDatastore -OutputFile C:\Temp\MyFile.csv
	.EXAMPLE
		Get-VMperNFSDatastore -OutputFile C:\Temp\MyFile.csv -DataCenter mydatacenter
	.PARAMETER DataCenter
	.PARAMETER OutputFile
	#>
	[CmdletBinding()]
	param
	(
		[Parameter(Mandatory=$false)]
		[string]$DataCenter,

		[Parameter(Mandatory=$false)]
		[string]$OutputFile
	)

	if($DataCenter)
	{
		$oPut=Get-Datacenter $DataCenter | Get-Datastore | Where {$_.Type -eq "NFS"} | Select Name,@{N="NumVM";E={@($_ | Get-VM).Count}} | Sort NumVM,Name
	}
	else
	{
		$oPut=Get-Datastore | Where {$_.Type -eq "NFS"} | Select Datacenter,Name,@{N="NumVM";E={@($_ | Get-VM).Count}} | Sort Datacenter,NumVM,Name
	}

	if($OutputFile)
	{
		$oPut | Export-Csv -Path $OutputFile -NoTypeInformation
	}
	else
	{
		$oPut | Format-Table
	}
}

function Add-NFSDataStoreToDataCenter
{
	<#
	.SYNOPSIS
		[PowerCLI]Mounts an NFS store on each host in a given DataCenter
	.DESCRIPTION
		Mounts an NFS store on each host in a given DataCenter
	.EXAMPLE
		Add-NFSDataStoreFromDataCenter -DataCenterName "dc1" -DataStoreName "ds1" -NFSHost 192.168.12.12 -NFSRemoteVolume "/vol/ds1"
	.EXAMPLE
		Add-NFSDataStoreFromDataCenter -DataCenterName "dc1" -DataStoreName "ds1" -NFSHost 192.168.12.12 -NFSRemoteVolume "/vol/ds1" -ReadOnly
	.PARAMETER Cluster
		The name of the Cluster you need information for
	.PARAMETER DatastoreName
		Name of the Datastore in which you want to add
	.PARAMETER NFSHost
		Name/IP of the remote NFS host
	.PARAMETER NFSRemoteVolume
		Volume export on remote host
	.PARAMETER ReadOnlyVolume
		Switch parameter to decide of the volume should be Readonly or Read/Write.  Default is Read/Write
	#>
	param
	(
		[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Name of the Datastore in which you want to add")]
		[string]$DatastoreName,

		[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="The name of the Cluster you need information for")]
		[string]$Cluster,

		[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Name/IP of the remote NFS host")]
		[string]$NFSHost,

		[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Volume export on remote host")]
		[string]$NFSRemoteVolume,

		[Parameter(Mandatory=$false,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Switch parameter to decide of the volume should be Readonly or Read/Write.  Default is Read/Write")]
		[switch]$ReadOnlyVolume=$false
	)

	foreach ($vmHost in (Get-Cluster $Cluster | Get-VMHost))
	{
		$dataStore = $vmhost | Get-Datastore $DatastoreName -ErrorAction SilentlyContinue
		if(!$dataStore)
		{
			$vmHost | New-Datastore -Nfs -NfsHost $NFSHost -Path $NFSRemoteVolume.TrimStart("\") -ReadOnly:$ReadOnlyVolume -Name $DatastoreName
		}
	}
}

function Get-FibreWWN
{
	<#
	.SYNOPSIS
		[PowerCLI]Gathers and formats the WWN ports for the FibreChannel HBA's of all hosts in a given datacenter
	.DESCRIPTION
		Gathers and formats the WWN ports for the FibreChannel HBA's of all hosts in a given datacenter
	.EXAMPLE
		Get-FibreWWN -DataCenterName "dc1"
	.PARAMETER Cluster
		The name of the Cluster you need information for
	#>
	[CmdletBinding()]
	param
	(
    	[Parameter(Mandatory=$true,
		ValueFromPipeline=$true,
		ValueFromPipelinebyPropertyName=$True,
		HelpMessage="Name of the Cluster that contains the host you wish to act on")]
    	$Cluster
	)

    $list = Get-Cluster $Cluster | Get-VMHost | Get-VMHostHBA -Type FibreChannel | Select VMHost,Device,@{N="WWN";E={"{0:X}"-f$_.PortWorldWideName}} | Sort VMhost,Device

    #Go through each row and put : between every 2 digits
    foreach ($item in $list)
    {
        $item.wwn = (&{for ($i=0;$i-lt$item.wwn.length;$i+=2)
            {
                $item.wwn.substring($i,2)
            }}) -join':'
    }

    return $list
}

function Get-NFSVMK
{
    param
    (
        <#
        .SYNOPSIS
		    [PowerCLI]Gets the list of the VMKernel IP addresses for the NFS networks for each host
        .DESCRIPTION
            Gets the list of the VMKernel IP addresses for the NFS networks for each host
        .PARAMETER VMHosts
            Comma-delimeted list of hosts
        .EXAMPLE
            get-nfsvmk -vmhosts "myhost.somewehre.com"
        #>
        [Parameter(Mandatory=$true)]
        [string[]]$VMHosts
    )

    $objResults=foreach($vmhost in $vmhosts)
    {
        $tmpObj = Get-VMHost $vmhost
		if($tmpObj.Version -match "5.5")
		{
			$tmpIP = $tmpObj | Get-VMHostNetworkAdapter -VMKernel | where {$_.VMotionEnabled -eq $false -and $_.FaultToleranceLoggingEnabled -eq $false -and $_.ManagementTrafficEnabled -eq $false} | select IP
		}
		else
		{
			$tmpIP = $tmpObj | Get-VMHostNetworkAdapter -VMKernel | where {$_.VMotionEnabled -eq $false -and $_.FaultToleranceLoggingEnabled -eq $false -and $_.ManagementTrafficEnabled -eq $false} | select IP
		}


        new-object PSObject -Property @{
                HostName=$tmpObj.Name
                NFSIP=$tmpIP.IP
            }
    }

    return $objResults

}

function Get-ThinProvisionedVM
{
    <#
	.SYNOPSIS
		[PowerCLI]Gathers all VMs that has a Thin Provisioned VMDK
	.DESCRIPTION
		Gathers all VMs that has a Thin Provisioned VMDK
	.EXAMPLE
		Get-ThinProvisionedVM
	#>
	[CmdletBinding()]
	param
	(
		[object]$VMs
	)

	if(!$VMs)
	{
		$VMs = Get-VM
	}

    $outObj = foreach ($vm in $VMs)
    {
        $view = Get-View $vm

        if ($view.config.hardware.Device.Backing.ThinProvisioned -eq $true)
        {
            foreach($device in ($view.Config.Hardware.Device | where {($_.GetType()).Name -eq "VirtualDisk"}))
            {
					if($device.Backing.Filename)
					{
                New-Object PSObject -Property @{
                    Name = $vm.Name
                    Provisioned = [math]::round($vm.ProvisionedSpaceGB , 2)
                    Total = [math]::round(($device | Measure-Object CapacityInKB -Sum).sum/1048576 , 2)
                    Used = [math]::round($vm.UsedSpaceGB , 2)
                    VMDKs = $device.Backing.Filename | Out-String
                    VMDKSize = $device.CapacityinKB/1048576 | out-string
                    Thin = $device.Backing.ThinProvisioned | Out-String
					}
					}
            }
        }
    }
	return $outObj
}

function Attach-SANDatastore
{
    [CmdletBinding()]
    param
    (
        [Parameter(Mandatory = $false)]
        [string]$Datacenter
    )

    if ($datacenter)
    {
        $vmHosts = get-datacenter $datacenter | Get-VMHost
    }
    else
    {
        $vmHosts = Get-VMHost
    }
    foreach ($vmhost in $vmHosts)
    {
            #Re-attach to the host
        $detachedLuns = $vmhost | Get-VMHostHba -Type FibreChannel | Get-ScsiLun | Where {$_.ExtensionData.OperationalState -eq "off"}
        $storSys = get-view $vmhost.ExtensionData.ConfigManager.StorageSystem

        foreach ($detachedLun in $detachedLuns)
        {
            $storSys.AttachScsiLun($detachedLun.ExtensionData.Uuid)
            $storSys.RescanAllHba()
        }

            #Now mount it to the host
            $vmhost = Get-VMHost $vmhost.Name

        $lunsNeedingMounting = $vmhost | Get-Datastore | Where State -eq "Unavailable"
        foreach ($lunNeedingMounting in $lunsNeedingMounting)
        {
            $storSys.MountVmfsVolume($lunNeedingMounting.ExtensionData.Info.vmfs.uuid)
        }
    }
}

function Detach-SANDatastore
{
    [CmdletBinding()]
    param
    (
        [Parameter(ValueFromPipeline = $true, Mandatory = $true)]
        [VMware.VimAutomation.ViCore.Types.V1.DatastoreManagement.Datastore]$Datastore,

        [Parameter(Mandatory = $false)]
        [string]$Datacenter
    )

	foreach($ds in $Datastore)
	{
        $hostviewDSDiskName = $ds.ExtensionData.Info.vmfs.extent[0].Diskname
        if($ds.ExtensionData.Host)
        {
            $attachedHosts = $ds.ExtensionData.host
            foreach($VMHost in $attachedHosts)
            {
                $dataCenterObj = get-vmhost -Id ("HostSystem-$($VMHost.key.value)") | Get-Datacenter

                [bool]$Proceed = $true

                if($Datacenter)
                {
                    if($dataCenterObj.Name -ne $Datacenter)
                    {
                        $Proceed = $false
                    }
                }

                if($Proceed)
                {
                    $hostView = Get-View $vmHost.Key
                    $StorageSys = Get-View $hostView.ConfigManager.StorageSystem
                    $devices = $StorageSys.StorageDeviceInfo.ScsiLun

                    foreach($device in $devices)
                    {
                        if ($device.canonicalName -eq $hostviewDSDiskName)
                        {
                            Write-Verbose "Unmounting LUN $($Device.canonicalName) from host $($hostview.Name)..."
                            try
                            {
                                $StorageSys.UnmountVMFSVolume($ds.ExtensionData.Info.vmfs.uuid)
                            }
                            catch
                            {

                            }

                            $LunUUID = $Device.Uuid
                            Write-Verbose "Detaching LUN $($Device.canonicalName) from host $($hostview.Name)..."
                            $StorageSys.DetachScsiLun($LunUUID)
                            $StorageSys.RescanAllHba()
                        }
                    }
                }
            }
        }
    }
}