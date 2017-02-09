#copyright.yuan
import cassandra
from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.auth import PlainTextAuthProvider
from flask import Flask
from flask import request
import time

##############################################################################################################
#                               connect to cassandra cluster bycassandra-driver                              #
##############################################################################################################
def connect_cassandra():
    cluster = Cluster(
        contact_points=[
            "35.165.161.176", "52.10.200.87", "35.163.204.211" # AWS_VPC_US_WEST_2 (Amazon Web Services (VPC))
        ],
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc='AWS_VPC_US_WEST_2'), # your local data centre
        port=9042,
            auth_provider = PlainTextAuthProvider(username='iccassandra', password='39435c7ec35d137d86cfe94dbcb36989')
    )
    return cluster


##############################################################################################################
#                                            excecute query function                                         #
##############################################################################################################
def search_cassandra(cluster,keyspace,query):
    session = cluster.connect(keyspace)
    #print('Connected to cluster %s' % cluster.metadata.cluster_name)
    session.execute('USE %s;' % keyspace)
    session.row_factory = cassandra.query.tuple_factory
    try:
        result = session.execute(query)
#   for host in cluster.metadata.all_hosts():
#       print ('Datacenter: %s; Host: %s; Rack: %s' % (host.datacenter, host.address, host.rack))
#   cluster.shutdown()
    except cassandra.protocol.SyntaxException:
        result = ["Syntax error in CQL query"]
    return result


##############################################################################################################
#                               parse query results to html-readable type                                    #
##############################################################################################################
def result_parse(result_set):
    #print(result_set)
    if result_set == ['Syntax error in CQL query']:
        result = '''Syntax&nbsperror&nbspin&nbspCQL&nbspquery.'''
    else:
        amount = 0
        result = '''<table  id="table" width="100%" border="2"><tr><td>dc_key</td><td>dc_dist</td><td>dispatch_date</td><td>dispatch_time</td><td>
                    hour</td><td>lat</td><td>location_block</td><td>lon</td><td>month</td><td>
                    police_districts</td><td>psa</td><td>text_general_code</td><td>ucr_general</td></tr>'''#header of final table
        for line in result_set:
            amount += 1
            line = '</td><td>'.join(str(x) for x in line)
            result += "<tr><td>%s</td></tr>"%line
        result = '''<p style="font-family:verdana">
                                Amount :''' + str(amount) + '''</p>''' +result
    return result


##############################################################################################################
#                                         create the web page                                                #
##############################################################################################################
#design html for result page
def result_page_design(query_criteria,result,elapsed_time):
    return'''<form action="/" method="post">
            <head>
                <style>
                #header {
                    background-color:black;
                    color:white;
                    text-align:center;
                    padding:5px;
                }
                #nav {
                    line-height:20px;
                    font-family:verdana;
                    background-color:#eeeeee;
                    text-align:center;
                    font-family:verdana;
                    font-size:20px;
                    color:grey;
                    height:auto!important;
                    height:450px;
                    min-height:450px;
                    width:100%;
                    padding:5px;
                }
                #table{
                background-color:white;
                text-align:center;
                font-size:12px;
                line-height:40px;
                padding:5px;
                }
                #button {
                    font-family:verdana;
                    background-color:#eeeeee;
                    clear:both;
                    text-align:center;
                    padding:5px;
                    height:50px;
                    line-height:50px;
                }
                #footer {
                    background-color:black;
                    color:white;
                    clear:both;
                    text-align:center;
                    padding:5px;
                }
                </style>
            </head>
                <body>
                    <div id="header">
                        <h1>Crime Search</h1>
                    </div>
                    <div id="nav">
                        '''+ query_criteria +'''

                         '''+ elapsed_time +'''
                        </p>
                        '''+result+'''
                        </table>
                    </div>
                    <div style="clear:both"></div>
                    <div id="button">
                        <a href="/";”>Back and search more</a>
                    </div>
                    <div id="footer">
                        Copyright >> Yuan Zhuang-yn770354@dal.ca
                    </div>
                </body>
            </form>'''


application = Flask(__name__)
@application.route('/', methods=['GET'])
def home():
    #design search page
    return '''<form action="/" method="post">
    <head>
        <style>
        #header {
            background-color:black;
            color:white;
            text-align:center;
            padding:5px;
        }
        #nav {
            line-height:30px;
            font-family:verdana;
            background-color:#eeeeee;
            height:300px;
            width:100%;
            padding:5px;
        }
        #table{
            background-color:#eeeeee;
            text-align:center;
            font-size:15px;
            color:grey
            line-height:30px;
            padding-left:200px;
            padding-right:200px;
        }
        #button {
            font-family:verdana;
            color:purple;
            font-size:13px;
            clear:both;
            text-align:center;
            padding:15px;
            height:200px;
            line-height:10px;
        }
        #footer {
            background-color:black;
            vertical-align:top;
            color:white;
            clear:both;
            text-align:center;
            padding:5px;
        }
        </style>
    </head>
    <body>
        <div id="header">
            <h1>Crime Search</h1>
        </div>
        <div id="nav">
            <table id="table" width="100%">
                <tr><td>Dc_Key:</td><td>Psa:</td><td>Dispatch_Date:</td></tr>
                <tr>
                  <td><input name="Dc_Key"></td>
                  <td><input name="Psa"></td>
                  <td><input name="Dispatch_Date"></td>
                </tr>
                <tr><td>&nbsp</td></tr>
                <tr><td>Dispatch_Time:</td><td>Dc_Dist:</td><td>Location_Block:</td></tr>
                <tr>
                  <td><input name="Dispatch_Time"></td>
                  <td><input name="Dc_Dist"></td>
                  <td><input name="Location_Block"></td>
                </tr>
                <tr><td>&nbsp</td></tr>
                <tr><td>Police_Districts:</td><td>Month:</td><td>Hour:</td></tr>
                <tr>
                  <td><input name="Police_Districts"></td>
                  <td><input name="Month"></td>
                  <td><input name="Hour"></td>
                <tr><td>&nbsp</td></tr>
                <tr><td>UCR_General:</td><td>Text_General_Code:</td></tr>
                <tr>
                  <td><input name="UCR_General"></td>
                  <td><input name="Text_General_Code"></td>
                </tr>
            </table>
        </div>
        <div id="button">
            <button type="submit" style="font-size:20px">search</button>
            <p>example of search type:</p>
            <p>Dc_Key:200918067518&nbsp&nbsp&nbsp&nbsp&nbsp&nbspPsa:3&nbsp&nbsp&nbsp&nbsp&nbsp&nbsp&nbspDispatch_Date:2009-10-02</p>
            <p>Dispatch_Time:14:24:00.000000000&nbsp&nbsp&nbspDc_Dist:18&nbsp&nbsp&nbspLocation_Block:S 38TH ST / MARKETUT ST</p>
            <p>Police_Districts:20&nbsp&nbsp&nbsp&nbsp&nbsp&nbspMonth:2009-10&nbsp&nbsp&nbsp&nbsp&nbsp&nbspHour:14</p>
            <p>UCR_General:800&nbsp&nbsp&nbspText_General_Code:Other Assaults</p>
        </div>
        <div id="footer">
            Copyright >> Yuan Zhuang-yn770354@dal.ca
        </div>
    </body>
    </form>'''



@application.route('/', methods=['POST'])
def query():
    # specify text type
    raw_query_items_1 = ['Dc_Dist', 'Hour', 'UCR_General', 'Police_Districts']
    raw_query_items_2 = ['Psa','Dispatch_Date','Dispatch_Time','Dc_Key','Location_Block',
                       'Text_General_Code','Month']
    # generate query sentence
    query_criteria = []
    for item in raw_query_items_1:
        if request.form[item] != '' :
               query_criteria.append(" %s = %s and" % (item,request.form[item]))
    for item in raw_query_items_2:
        if request.form[item] != '':
               query_criteria.append(" %s = \'%s\' and" % (item, request.form[item]))
    query_text = ''.join(query_criteria)
    query_text = query_text[:-3]

    query_criteria = '''<p style="font-family:verdana">
                                Query criteria :''' + query_text + '''</p>'''
    query_text = "SELECT * FROM crime WHERE %s ALLOW FILTERING;" % query_text
    # connect cluster and implement search function
    cluster = connect_cassandra()
    start_time = time.time()
    result = search_cassandra(cluster,"cassandra_community",query_text)
    stop_time = time.time()
    elapsed_time = '''<p style="font-family:verdana">
                            Elapsed time :'''+str(stop_time - start_time)+'''</p>'''

    #transmit result to web page
    return (result_page_design(query_criteria,result_parse(result),elapsed_time))

#Restful design
@application.route('/<query_criteria>', methods=['GET'])
def query1(query_criteria):
    query_text = "SELECT * FROM crime WHERE %s ALLOW FILTERING;" % query_criteria
    # connect cluster and implement search function
    cluster = connect_cassandra()
    start_time = time.time()
    result = search_cassandra(cluster, "cassandra_community", query_text)
    query_criteria_ = '''<p style="font-family:verdana">
                                Query criteria :''' + query_criteria + '''</p>'''
    stop_time = time.time()
    elapsed_time = '''<p style="font-family:verdana">
                                Elapsed time :''' + str(stop_time - start_time) + '''</p>'''
    #print("result = ", result)
    return (result_page_design(query_criteria_, result_parse(result), elapsed_time))


@application.errorhandler(400)
def handle_invalid_usage(error):
    return (result_page_design('','<h1>Bad Request 400</h1>','')),400

@application.errorhandler(500)
def internal_error(error):
    return (result_page_design('','<h1>Bad Request 500</h1>','')), 500


if __name__ == '__main__':
    application.run(host='0.0.0.0')