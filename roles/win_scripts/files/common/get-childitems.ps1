[cmdletbinding()]
    param(
        [Parameter(Mandatory)]
        [string]$pathname)

 
Get-ChildItem -path $pathname -Directory |
Select-Object FullName |
ForEach-Object -Process{New-Object -TypeName PSObject -Property @{Name =$_.FullName;
    Size = (Get-ChildItem -path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue |
    Measure-Object -Property Length -Sum ).Sum/1024MB}} | Select-Object Name, @{Name="Size(GB)";Expression={("{0:N2}" -f($_.Size))}} | ft -autosize
