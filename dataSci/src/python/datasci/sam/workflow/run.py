import subprocess
import sys
import time
import os
import commands
from boto.emr.connection import EmrConnection

S3_JAR_DIR = os.environ['S3HOME'] + '/' + os.environ['LOGNAME'] + '/jars'

class EmrStatusPoller(object):
    
    def __init__(self,connection,interval=120):
        self.connection = connection
        self.interval = interval
        self.last = 0
    
    def poll_state(self,job_id):
        next = self.last + self.interval
        now = time.time()
        if now < next:
            time.sleep(next - now)
        
        status = conn.describe_jobflow(job_id).state
        self.last = time.time()
        return status
    
def parse_job_id(output_string):
    return output_string.strip().split(' ')[3]

def put_jar(local_path,jar_name):
    template = 's3cmd put -v %s %s/%s' % (local_path,S3_HOME,jar_name)
    code = subprocess.call(['s3cmd put -v',local_path,S3_HOME + '/' + jar_name])
    return code

def create_flow(remote_jar_name,signals_output):
    remote_jar_path = S3_JAR_DIR + '/' + remote_jar_name 
    signals_job_template = commands.qs_sig_gen_template
    signals_job_cmd = commands.modify(signals_job_template,'-outputUrl',signals_output)
    signals_job_cmd = commands.modify(signals_job_cmd,'-overallSampleRateDecimal',0.5)
    signals_job_cmd = commands.modify(signals_job_cmd,'-jar',remote_jar_path)
    signals_job_cmd_str = commands.make_command_string(signals_job_cmd)
    
    lr_job_template = commands.log_reg_template
    lr_job_cmd = commands.modify(lr_job_template,'-regularization',2.0)
    lr_job_cmd = commands.modify(lr_job_cmd,'--inputTrain',signals_output + '/training/normalized_liblinear_sample.gz/')
    lr_job_cmd = commands.modify(lr_job_cmd,'--inputTest',signals_output + '/testing/normalized_liblinear.gz/')
    lr_job_cmd_str = commands.make_command_string(lr_job_cmd)
    
    return remote_jar_path,signals_job_cmd_str,lr_job_cmd_str

def launch_and_monitor(command_string,job_name):
    proc = subprocess.Popen(['elastic-mapreduce --create ' + command_string], stdout=subprocess.PIPE, shell=True)
    output = proc.communicate()[0]
    print output
    job_id = parse_job_id(output)
    print "%s Job Id: %s" % (job_name,job_id)
    poller = EmrStatusPoller(conn,120)
    while True:
        time.sleep(10)
        state = poller.poll_state(job_id)
        print "%s state: %s" % (job_name,state)
        if state == 'COMPLETED':
            break

if __name__ == '__main__':
    conn = EmrConnection()
    remote_jar_path,signals_job_cmd_str,lr_job_cmd_str = create_flow('jobRunner-hadoop.jar',
                                                                     's3n://intentmedia-hawk-output/sam.ross/cascalog/20150310-generate-qs-signals-overall-sr')
    print remote_jar_path
    print signals_job_cmd_str
    print lr_job_cmd_str
    
    launch_and_monitor(signals_job_cmd_str,'SIGNALS')
    launch_and_monitor(lr_job_cmd_str,'REGRESSION')


    


                

            
