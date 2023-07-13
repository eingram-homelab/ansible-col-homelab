$TaskSequenceName = "Install NotepadPP"
$SoftwareDistributionPolicy = Get-WmiObject -Namespace "root\ccm\policy\machine\actualconfig" -Class "CCM_SoftwareDistribution" | Where-Object { $_.PKG_Name -like $TaskSequenceName } | Select-Object -Property PKG_PackageID,ADV_AdvertisementID
$ScheduleID = Get-WmiObject -Namespace "root\ccm\scheduler" -Class "CCM_Scheduler_History" | Where-Object { $_.ScheduleID -like "*$($SoftwareDistributionPolicy.PKG_PackageID)*" } | Select-Object -ExpandProperty ScheduleID
$TaskSequencePolicy = Get-WmiObject -Namespace "root\ccm\policy\machine\actualconfig" -Class "CCM_TaskSequence" | Where-Object { $_.ADV_AdvertisementID -like $SoftwareDistributionPolicy.ADV_AdvertisementID }
$TaskSequencePolicy.ADV_MandatoryAssignments = $true
$TaskSequencePolicy.Put()

Invoke-WmiMethod -Namespace "root\ccm" -Class "SMS_Client" -Name "TriggerSchedule" -ArgumentList $ScheduleID