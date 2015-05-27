import sys

qs_sig_gen_template = (
    ('--jar','s3://intentmedia-hawk-output/sam.ross/jars/jobRunner-hadoop.jar',None),
('--instance-group', 'master',None), 
('--instance-type', 'c3.4xlarge',None),
('--instance-count', 1,None),
('--instance-group', 'core',None),
('--instance-type', 'c3.8xlarge',None),
('--instance-count', 19,None),
('--main-class', 'tasks.generate_signals_quality_score',None),
('--args', (
 ('-stepInputBaseUrl','s3n://intentmedia-hadoop-production/input/',None),
 ('-outputUrl','s3n://intentmedia-hawk-output/sam.ross/cascalog/20150218-generate-qs-signals-overall-sr',None),
 ('-testingDateFrom','20150125',None),
 ('-testingDateTo','20150125',None),
 ('-trainingDateFrom','20150118',None),
 ('-trainingDateTo','20150124',None),
 ('-productCategoryType','FLIGHTS',None),
 ('-adType','CT',None),
 ('-overallSampleRateDecimal',0.25,None),
 ('-nonClickSampleRateDecimal',0.015,None),
 ('-featureSet','ALL_FEATURES',None))))

log_reg_template = (
    ('--name', '"SR-71:test"',None),
    ('--instance-group', 'master',None),
    ('--instance-type', 'r3.2xlarge',None),
('--instance-count', 1,None),
('--instance-group', 'core',None), 
('--instance-type', 'r3.8xlarge',None),
('--instance-count', 5,None),
('--bid-price', 0.75,None),
('--bootstrap-action', 's3://intentmedia-hawk-output/sam.ross/scripts2/sr-spark-bootstrap-snap.sh',None), 
('--jar', 's3://elasticmapreduce/libs/script-runner/script-runner.jar',None),
('--step-name', '"test"',None),
(None,'s3://intentmedia-hawk-output/sam.ross/scripts2/run-spark-job-0.1.0.sh','ARG_STYLE'),
(None,'s3://intentmedia-hawk-output/sam.ross/jars/rf-0.0.1.jar','ARG_STYLE'),
(None,'com.intentmedia.spark.BFGSJob','ARG_STYLE'),
('--inputTrain','s3://intentmedia-hawk-output/sam.ross/cascalog/20150210-generate-qs-signals-overall-sr/training/normalized_liblinear_sample.gz/','ARG_STYLE'),
('--inputTest','s3://intentmedia-hawk-output/sam.ross/cascalog/20150210-generate-qs-signals-overall-sr/testing/normalized_liblinear.gz/','ARG_STYLE'),
('--regularization',0.1,'ARG_STYLE'),
('--hashDim',50000,'ARG_STYLE'),
('--output','s3://intentmedia-hawk-output/sam.ross/output/20150305-qs-bfgs-50000-zeropoint1','ARG_STYLE'),
('--executorMemory','200g','ARG_STYLE'))

def make_command_string(command):
    command_string = ''
    for item in command:
        if item[0] == '--args':
            command_string += '--args "'
            for key,val,_ in item[1]:
                command_string += key + ',' + str(val) + ','
            command_string += '"'
        else:
            if item[2] == 'ARG_STYLE':
                if item[0] is not None:
                    command_string += '--arg ' + item[0] + ' --arg ' + str(item[1]) + ' ' 
                else:
                    command_string += '--arg ' + item[1] + ' '
            else:
                command_string += item[0] + ' ' + str(item[1]) + ' '
    
    command_string = command_string.replace(',"','"')
    command_string = command_string.strip(',')
    return command_string
    #return 'elastic-mapreduce --create ' + command_string

def modify(command,arg_name,new_val):
    new_command = []
    for item in command:
        if item[0] == '--args':
            new_sub_command = []
            for sub_item in item[1]:
                if sub_item[0] == arg_name:
                    new_sub_command.append((arg_name,new_val,sub_item[2]))
                else:
                    new_sub_command.append(sub_item)
            new_command.append(('--args',new_sub_command))

        elif item[0] == arg_name:
            new_command.append((arg_name,new_val,item[2]))

        else:
            new_command.append(item)

    return new_command
    #return make_command_string(new_command)

if __name__ == '__main__':
    print make_command_string(log_reg_template)
    print ''
    print make_command_string(qs_sig_gen_template)
    print ''
    print modify(log_reg_template,'--regularization',10000)
    print ''
    print modify(log_reg_template,'--bid-price','FUCK')
    print ''
    print modify(log_reg_template,'--doesnotexist','FUCK')
    print ''
    print modify(qs_sig_gen_template,'--instance-count',10000000)
    print ''
    print modify(qs_sig_gen_template,'-featureSet','NO_GODDAM_FEATURES')

