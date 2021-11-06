startsection()
{
    if [ -z "$3" ]; then
        echo -e "\e[0Ksection_start:`date +%s`:$1\r\e[0K\e[1m$2\e[0m"
    else
        echo -e "\e[0Ksection_start:`date +%s`:$1[collapsed=true]\r\e[0K\e[1m$2\e[0m"
    fi
}

endsection()
{
    echo -e "\e[0Ksection_end:`date +%s`:$1\r\e[0K"
}
