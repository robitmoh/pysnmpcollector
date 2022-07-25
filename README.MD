

install
apt install python3-pip python3-venv  virtualenv python3-virtualenv

virtualenv pycollector
source pycollector/bin/activate


adduser --system pycollector
addgroup --system pycollector

    sudo systemctl daemon-reload

    sudo systemctl enable “service-name”
    #In my case is sudo systemctl enable temperature.service

    sudo systemctl start “service-name”
    #In my case is sudo systemctl start temperature.service