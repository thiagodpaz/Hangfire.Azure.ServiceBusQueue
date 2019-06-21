Include "packages\Hangfire.Build.0.2.6\tools\psake-common.ps1"

Task Default -Depends Collect
Task CI -Depends Pack

Task Build -Depends Clean -Description "Restore all the packages and build the whole solution." {
    Exec { dotnet build -c Release }
}

Task Collect -Depends Build -Description "Copy all artifacts to the build folder." {
    Collect-Assembly "Hangfire.Azure.ServiceBusQueue" "netstandard2.0"
}

Task Pack -Depends Collect -Description "Create NuGet packages and archive files." {
    $version = Get-PackageVersion

    Create-Archive "Hangfire.Azure.ServiceBusQueue-$version"
	Exec { dotnet pack -c Release -p:PackageVersion=$version -o "$build_dir" --include-symbols }
}