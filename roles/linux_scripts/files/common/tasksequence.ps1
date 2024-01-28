# Retrieve the name of the task sequence that should be executed
$TaskSequenceName = "Install NotepadPP"
$SoftwareCenter = New-Object -ComObject "UIResource.UIResourceMgr"
#Retrieve Task Sequence details from Software Center
$TaskSequence = $softwareCenter.GetAvailableApplications() | Where-Object -Filterscript { $_.PackageName -eq "$TaskSequenceName" }
#If Task Sequence advertisement found then execute the task sequence:
If($TaskSequence){
$SoftwareCenter.ExecuteProgram($TaskSequence.Id,$TaskSequence.PackageId,$true)
}