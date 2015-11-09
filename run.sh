if pgrep "redis" > /dev/null
then echo "Redis is already up"
else
    nohup redis-server &> redis-server.out&
    echo "Started redis server."
fi

if pgrep "vyked-registry" > /dev/null
then echo "Vyked registry is already up"
else
    nohup python3 -m vyked.registry &> registry.out&
    echo "Started Vyked registry."
fi

    
