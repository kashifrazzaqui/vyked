FROM docker2.1mg.com:5000/1mg/vyked
ADD . /opt/vyked
RUN cd /opt/ && pip3 install -e vyked
