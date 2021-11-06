function global:startsection($id, $title, $collapsed) {
    $date = [DateTimeOffset]::Now.ToUnixTimeSeconds()
    #Oh my god PowerShell really wants to be the best peace of shit shell of all times. Seriously even the raw sh from linux does not have so many defects!
    $ESC = [char] 27 
    $CR = [char] 13

    if ( $collapsed -eq $null ) {
        echo "${ESC}[0Ksection_start:${date}:${id}${CR}${ESC}[0K${ESC}[1m${title}${ESC}[0m"
    } else {
        echo "${ESC}[0Ksection_start:${date}:${id}[collapsed=true]${CR}${ESC}[0K${ESC}[1m${title}${ESC}[0m"
    }
}

function global:endsection($id) {
    $date = [DateTimeOffset]::Now.ToUnixTimeSeconds()
    $ESC = [char] 27 
    $CR = [char] 13

    echo "${ESC}[0Ksection_end:${date}:${id}${CR}${ESC}[0K"
}
