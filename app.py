# doing necessary imports
import threading
import io
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure

from logger_class import getLog
from flask import Flask, render_template, request, jsonify, Response, url_for, redirect
from flask_cors import CORS, cross_origin
import pandas as pd
from FlipkratScrapping import FlipkratScrapper
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import cassandra
from cassandraDBOperations import CassaDBManagement
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import pandas as pd
import plotly.express as px
import plotly.offline as py
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot
#commit2

#commit changes
#changes commit




rows = {}
keyspace_name = 'ineuron3'

logger = getLog('flipkrat.py')

free_status = True
db_name = 'review'

app = Flask(__name__)  # initialising the flask app with the name 'app'

#For selenium driver implementation on heroku
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--disable-gpu')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument("disable-dev-shm-usage")

#To avoid the time out issue on heroku
class threadClass:

    def __init__(self, expected_review, searchString, scrapper_object, review_count):
        self.expected_review = expected_review
        self.searchString = searchString
        self.scrapper_object = scrapper_object
        self.review_count = review_count
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True  # Daemonize thread
        thread.start()  # Start the execution

    def run(self):
        global db_name, free_status
        free_status = False
        db_name = self.scrapper_object.getReviewsToDisplay(expected_review=self.expected_review,
                                                                   username='DzgSXTSzQgWNpYocAWPpQzAX',
                                                                   password='27C9coC--crqmF0MiZldjv9Kg8NyhTzMP66SOPbHtiaNOWcidhyBz1FuOIuUp.,p2CajK266pu2QEhLkCNs4Zkt6qQaSce2cS_+10a9clpH6UhkdUkNtuBoTczw8sK_X',
                                                                   searchString=self.searchString,
                                                                   review_count=self.review_count)
        logger.info("Thread run completed")
        free_status = True


@app.route('/', methods=['POST', 'GET'])
@cross_origin()
def index():
    if request.method == 'POST':
        global free_status
        ## To maintain the internal server issue on heroku
        if free_status != True:
            return "This website is executing some process. Kindly try after some time..."
        else:
            free_status = True
        searchString = request.form['content'].replace(" ", "")  # obtaining the search string entered in the form
        expected_review = int(request.form['expected_review'])
        try:
            review_count = 0
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options)
            CassaDB = CassaDBManagement(username='username', password='password')
            session1 = CassaDB.getCassaDBClientObject()
            scrapper_object.openUrl("https://www.flipkart.com/")
            logger.info("Url hitted")
            scrapper_object.login_popup_handle()
            logger.info("login popup handled")
            scrapper_object.searchProduct(searchString=searchString)
            logger.info(f"Search begins for {searchString}")


            if  CassaDB.isDatabasePresent(keyspace_nam=keyspace_name):
                    try:
                        #response =  session1.execute("select count(*) from ineuron3.review where product_searched = '+ {} + ';".format(searchString)).one()
                        response1 = CassaDB.getDetailfromDatabase(searchString = searchString,response='response')
                    except Exception as e:
                        raise Exception("select error - Something" + str(e))


                    reviews = [i for i in response1]
                    #db_name = None


                    if len(reviews) > expected_review:
                        result = [reviews[i] for i in range(0, expected_review)]
                        scrapper_object.saveDataFrameToFile(file_name="static/scrapper_data.csv",
                                                        dataframe=pd.DataFrame(result))
                        logger.info("Data saved in scrapper file")
                        data = pd.read_csv("static/scrapper_data.csv")
                        return render_template('results.html', rows=result)  # show the results to user

                    else:
                         review_count = len(reviews)
                         threadClass(expected_review=expected_review, searchString=searchString,
                                scrapper_object=scrapper_object, review_count=review_count)

                         logger.info("data saved in scrapper file")
                         return redirect(url_for('feedback'))
            else:
                CassaDB.createDatabase(db_name=db_name)
                threadClass(expected_review=expected_review, searchString=searchString, scrapper_object=scrapper_object,
                            review_count=review_count)
                return redirect(url_for('feedback'))

        except Exception as e:
            raise Exception("(app.py) - Something went wrong while rendering all the details of product.\n" + str(e))

    else:
        return render_template('index.html')


@app.route('/feedback', methods=['GET'])
@cross_origin()
def feedback():
    try:
        global db_name


        if db_name is not None:
            scrapper_object = FlipkratScrapper(executable_path=ChromeDriverManager().install(),
                                               chrome_options=chrome_options)


            CassaDB = CassaDBManagement(username='username',password='password')
            session1 = CassaDB.getCassaDBClientObject()
            rows2 = []
            res={}
            res2 = []

            #if rows1[0] > 0:
            rows={}
                #rows2 = session1.execute("select comment,customer_name,discount_percent,emi_detail,offer_details,price,product_name,product_searched,ratings,review_age from ineuron3.review;")
            rows2 = CassaDB.getAllDetailfromDatabase(responseAll='responseAll')
            list1=['comment','customer_name','discount_percent','EMI','offer_details','price','product_name','product_searched','rating','review_age']

            for i in rows2:
                #res = dict(zip(list1,i))
                #res1 = [res]
                res2.append(dict(zip(list1,i)))
                res1 = res2

            reviews = [i for i in res1]


            #print(reviews)
            dataframe = pd.DataFrame(reviews)
            scrapper_object.saveDataFrameToFile(file_name="static/scrapper_data.csv", dataframe=dataframe)
            #collection_name = None
            data = pd.read_csv("static/scrapper_data.csv")
            df = pd.DataFrame(data=data)
            fig1 = px.scatter(df, y='rating', x='product_searched')
            py.plot(fig1, filename='scatterplot.png.html')
            return render_template('results.html', rows=res1)

        else:
            return render_template('results.html', rows=None)
            db_name = 'review'
    except Exception as e:
        raise Exception("(feedback) - Something went wrong on retrieving feedback.\n" + str(e))


@app.route("/graph", methods=['GET'])
@cross_origin()
def graph():
    return redirect(url_for('plot_png'))


@app.route('/a', methods=['GET'])
def plot_png():
    fig = create_figure()
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


def create_figure():
    data = pd.read_csv("static/scrapper_data.csv")
    dataframe = pd.DataFrame(data=data)
    fig = Figure()
    axis = fig.add_subplot(1, 1, 1)
    xs = dataframe['product_searched']
    ys = dataframe['rating']
    axis.scatter(xs, ys)
    return fig

if __name__ == "__main__":


    app.run()  # running the app on the local machine on port 8000
