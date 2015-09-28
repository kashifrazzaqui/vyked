if pgrep "vyked-registry" > /dev/null
    then kill -9 `pgrep vyked-registry`
    echo 'Killed vyked-registry'
fi

if pgrep "redis" > /dev/null
    then kill -9 `pgrep redis`
    echo 'Killed redis'
fi

