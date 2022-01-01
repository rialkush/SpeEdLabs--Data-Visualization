# for operations on data
import numpy as np
import pandas as pd
# for visualisation of data
import seaborn as sns
import matplotlib.pyplot as plt
# for making connection with database
import pyodbc
import pymssql
# for time
from datetime import datetime
# for making widgets
import ipywidgets as widgets
from IPython.display import display
# for statistical processing
from scipy import stats
# for input-output
import io
# for handling base64 image data
import base64
# for creating and saving interactive plots
import plotly
import json
import plotly.express as px

############ function to choose period ################

def choose_period():
    """This function asks the user to choose a period
 
     Parameters:
     No Argument
 
     Returns:
     returns the variable to uniquely determine the period
 
    """
    vis = int(input('(For visualisation w.r.t. Month/Quarter enter 1, else enter 2)  '))
    mo = 0    # keeps track of whether month or quarter is choosen
    # year_range = input("Please enter the year range in format (2012-2015): ")  # comment-out to ask user to select year-range
    y1 = 2016 #int(year_range[:4])
    y2 = 2030 #int(year_range[5:])
    
    # widget to choose year
    y_tup = [(str(i), str(i)) for i in range(y1, y2+1)]
    choosen_y = widgets.Dropdown(
        options = y_tup,
        value = '2021',
        description = 'Year:',
    )
    
    # widget to choose month
    m_tup = [('January', '01'), ('February', '02'), ('March', '03'), ('April', '04'), ('May', '05'), ('June', '06'), 
                     ('July', '07'), ('August', '08'), ('September', '09'), ('October', '10'), ('November', '11'), ('December', '12')]
    choosen_m = widgets.Dropdown(
        options = m_tup,
        value = '01',
        description = 'Month:',
    )
    
    # widget to choose quarter
    q_tup = [('Q1', 'Q1'), ('Q2', 'Q2'), ('Q3', 'Q3'), ('Q4', 'Q4')]
    choosen_q = widgets.Dropdown(
        options = q_tup,
        value = 'Q1',
        description = 'Quarter:',
    )
    
    # widget to choose start date
    sd = widgets.DatePicker(
        description = 'Start Date: ',
        disabled = False
    )
    # widget to choose end date
    ed = widgets.DatePicker(
        description = 'End Date: ',
        disabled = False
    )
    
    # display only desired widgets to user
    if(vis == 1):
        display(choosen_y)
        mo = int(input("Enter 1 to choose months and 2 for Quarters: "))
        if(mo == 1):
            display(choosen_m)
        else:
            display(choosen_q)
    else:
        display(sd)
        display(ed)
    return vis, mo, choosen_y, choosen_m, choosen_q, sd, ed

############ function to choose start & end dates corresponding to a period ################

def choose_start_end_date(vis, mo, choosen_y, choosen_m, choosen_q, sd, ed):
    """This function determines the start & end dates corresponding to a period
 
     Parameters:
     argument1 (int): reference of visualisation, for Month/Quarter enter 1, else enter 2
     argument2 (int): Enter 1 for months and 2 for Quarters
     argument3, 4, 5, 6, 7 (widgets): year, month, quarter, start date, end date widgets respectively
 
     Returns:
     datetime, datetime: Returns the start date & end dates corresponding to the period
 
    """
    start_date = datetime.now()
    end_date = datetime.now()
    # determine start & end dates
    if(vis == 2):
        start_date = datetime.combine(sd.value, datetime.min.time()) # add 00:00:00 time
        end_date = datetime.combine(ed.value, datetime.max.time())  # add 11:59:59 time
    elif (mo == 1):
        start_date = pd.to_datetime(choosen_y.value + '-' + choosen_m.value) 
        end_date = pd.to_datetime(choosen_y.value + '-' + choosen_m.value) + pd.offsets.MonthEnd()
        start_date = datetime.combine(pd.to_datetime(start_date.strftime('%Y-%m-%d')), datetime.min.time()) # add 00:00:00 time
        end_date = datetime.combine(pd.to_datetime(end_date.strftime('%Y-%m-%d')), datetime.max.time())  # add 11:59:59 time
    else:
        start_date = pd.to_datetime(choosen_y.value + '-' + choosen_q.value) 
        end_date = pd.to_datetime(choosen_y.value + '-' + choosen_q.value) + pd.offsets.QuarterEnd()
        start_date = datetime.combine(pd.to_datetime(start_date.strftime('%Y-%m-%d')), datetime.min.time()) # add 00:00:00 time
        end_date = datetime.combine(pd.to_datetime(end_date.strftime('%Y-%m-%d')), datetime.max.time())  # add 11:59:59 time
    
    print(f"Choosen Start Date is: {start_date}")
    print(f"Choosen End Date is: {end_date}")   
    
    return start_date, end_date

############ function to take dates from user DD-MM-YYYY and convert that into python datetime################

def choose_date(text):
    flag = 1
    while flag == 1 :
        try:
            date_entry = input(f'Enter {text} in DD-MM-YYYY format: ')
            day, month, year  = map(int, date_entry.split('-'))
            date = pd.to_datetime(datetime(year, month, day))
            flag = 0
            return date
        except:
            print("Please enter a valid date.")

            
############ function to convert base64 to png image ################            
            
def base64_to_png(byte, filename):
    decodeit = open(filename, 'wb')
    decodeit.write(base64.b64decode((byte)))
    decodeit.close()

############ function to extract virtual(statistical) min & max of data ################     

def get_min_max(data):
    mn = data.mean() - 2.5*data.std()
    mx = data.mean() + 2.5*data.std()
    return mn, mx
            
            
            

######### function to plot 2-histograms by splitting range ###########

def plot_histogram_range_split(data, xlab, ylab, ttl, l1, r1, b1, l2, r2, b2, plot_show, fname = './plots/dummy.jpg'):
    """This function plots two histograms by splitting the range of data for better visualisation
 
     Parameters:
     argument1 (array): data for which histogram to be plot
     argument2 (string): label of x-axis
     argument3 (string): label of y-axis
     argument4 (string): plot title
     argument5 (int): left boundary of range1
     argument6 (int): right boundary of range1
     argument7 (int): binwidth for range1
     argument8 (int): left boundary of range2
     argument9 (int): right boundary of range2
     argument5 (int): binwidth for range2
 
     Returns:
     None
 
    """
    
    # creating the bins for range1
    bin_width = b1
    bins_bounds = np.arange(l1, r1, bin_width)
    bin_values, idx = np.histogram(data, bins = bins_bounds)
    bin_centers = (bins_bounds[:-1] + bins_bounds[1:]) / 2
    
    plt.rcParams["figure.figsize"] = (40, 15)
    plt.rcParams.update({'font.size': 30})
    plt.style.use('seaborn-darkgrid')
    # plot histogram for range1
    plt.subplot(1, 2, 1)
    # create the map for colors
    norm = plt.Normalize(vmin = -max(bin_values)/5, vmax =  max(bin_values)/1.25)
    cmap = plt.cm.get_cmap('YlGnBu')
    colors = cmap(norm(bin_values))
    # plot the histogram(bar graph function is used just to account for color mapping)
    plt.bar(bin_centers, bin_values, bin_width, color = colors, edgecolor = 'b', linewidth = 0.5)
    plt.xlabel(xlab, fontweight="bold")
    plt.ylabel(ylab, fontweight="bold")
    plt.title(ttl, fontname="Times New Roman", size=45, fontweight="bold")
    # show the color map
    sm = plt.cm.ScalarMappable(cmap = 'YlGnBu', norm = norm)
    sm.set_array([])
    plt.colorbar(sm, orientation = 'vertical')
    
    # creating the bins for range1
    bin_width = b2
    bins_bounds = np.arange(l2, r2, bin_width)
    bin_values, idx = np.histogram(data, bins = bins_bounds)
    bin_centers = (bins_bounds[:-1] + bins_bounds[1:]) / 2
    
    # plot histogram for range2
    plt.subplot(1, 2, 2)
    # create the map for colors
    norm = plt.Normalize(vmin = -max(bin_values)/5, vmax =  max(bin_values)/1.25)
    colors = cmap(norm(bin_values))
    # plot the histogram(bar graph function is used just to account for color mapping)
    plt.bar(bin_centers, bin_values, bin_width, color = colors, edgecolor = 'b', linewidth = 0.5)
    plt.xlabel(xlab, fontweight="bold")
    plt.ylabel(ylab, fontweight="bold")
    plt.title(ttl, fontname="Times New Roman", size=45, fontweight="bold")
    # show the color map    
    sm = plt.cm.ScalarMappable(cmap = 'YlGnBu', norm = norm)
    sm.set_array([])
    plt.colorbar(sm, orientation = 'vertical')
    
    # save the base64 strings corresponding to PNG & SVG
    stringIObytes1 = io.BytesIO()
    plt.savefig(stringIObytes1, format='PNG')
    stringIObytes2 = io.BytesIO()
    plt.savefig(stringIObytes2, format='SVG')
    
    if(plot_show =='On'): plt.show()
    else: 
        plt.savefig(fname)
        plt.close()
    
    # get the base64 strings corresponding to PNG & SVG
    stringIObytes1.seek(0)
    base64PNG = base64.b64encode(stringIObytes1.read()).decode('ascii')
    stringIObytes2.seek(0)
    base64SVG = base64.b64encode(stringIObytes2.read()).decode('ascii')
    
    # make the plotly plots
    fig = px.histogram(data, title=ttl)
    fig = fig.to_json()
    info = json.loads(fig)
    plotlyData = info["data"]
    plotlyLayout = info["layout"]
    
    return base64PNG, base64SVG, plotlyData, plotlyLayout

######### function to plot the histogram on log-scale ###########

def plot_histogram_logscale(data, xlab, ylab, ttl, l, r, b, plot_show, fname = './plots/dummy.jpg'):
    """This function plots histogram on logscale for better visualisation of data
 
     Parameters:
     argument1 (array): data for which histogram to be plot
     argument2 (string): label of x-axis
     argument3 (string): label of y-axis
     argument4 (string): plot title
     argument5 (int): left boundary of range
     argument6 (int): right boundary of range
     argument7 (int): binwidth for range
 
     Returns:
     None
 
    """
    
    # creating the bins for range
    bin_width = b
    bins_bounds = np.arange(l, r, bin_width)
    bin_values, idx = np.histogram(data, bins = bins_bounds)
    bin_centers = (bins_bounds[:-1] + bins_bounds[1:]) / 2
    
    plt.rcParams["figure.figsize"] = (40, 15)
    plt.rcParams.update({'font.size': 30})
    # create the map for colors
    norm = plt.Normalize(vmin = -max(bin_values)/5, vmax =  max(bin_values)/1.25)
    cmap = plt.cm.get_cmap('YlGnBu')
    colors = cmap(norm(bin_values))
    # plot the histogram(bar graph function is used just to account for color mapping)
    plt.bar(bin_centers, bin_values, bin_width, color = colors, edgecolor = 'b', linewidth = 0.5)
    plt.yscale("log")
    plt.xlabel(xlab, fontweight="bold")
    plt.ylabel(ylab, fontweight="bold")
    plt.title(ttl, fontname="Times New Roman", size=45, fontweight="bold")
    # show the color map    
    sm = plt.cm.ScalarMappable(cmap = 'YlGnBu', norm = norm)
    sm.set_array([])
    plt.colorbar(sm, orientation = 'vertical')
    
    # save the base64 strings corresponding to PNG & SVG
    stringIObytes1 = io.BytesIO()
    plt.savefig(stringIObytes1, format='PNG')
    stringIObytes2 = io.BytesIO()
    plt.savefig(stringIObytes2, format='SVG')
    
    if(plot_show =='On'): plt.show()
    else: 
        plt.savefig(fname)
        plt.close()
    
    # get the base64 strings corresponding to PNG & SVG
    stringIObytes1.seek(0)
    base64PNG = base64.b64encode(stringIObytes1.read()).decode('ascii')
    stringIObytes2.seek(0)
    base64SVG = base64.b64encode(stringIObytes2.read()).decode('ascii')
    
    # make the plotly plots
    fig = px.histogram(data, log_y=True, title=ttl)
    fig = fig.to_json()
    info = json.loads(fig)
    plotlyData = info["data"]
    plotlyLayout = info["layout"]
    
    return base64PNG, base64SVG, plotlyData, plotlyLayout
    
    
######### function to plot heatmap ##########

def plot_heatmap(df, mn, mx, col, annot_fmt, ttl, plot_show, fname = './plots/dummy.jpg'):
    """This function makes the heatmap of the data
 
     Parameters:
     argument1 (Object): data for which heatmap to be plot
     argument2 (int): minimum color value
     argument3 (int): maximum color value
     argument4 (string): color scheme for heatmap
     argument5 (string): annotation format
     argument6 (string): title of heatmap
 
     Returns:
     None
 
    """
    row, column = df.shape
    
    plt.rcParams['figure.figsize'] = [40, 15]
    plt.rcParams.update({'font.size': 30})
    plt.rcParams["font.weight"] = "bold"
    plt.rcParams["axes.labelweight"] = "bold"
    sns.set(font_scale=2.5)
    # plot heatmap
    if((column>20) | (mx>10000)):
        sns.heatmap(df, vmin=mn, vmax=mx, cmap = col, linewidth = 0.05, linecolor = 'blue', annot = True, 
                fmt = annot_fmt, annot_kws={'fontsize':30, 'fontweight': 'bold', 'rotation': 90})
    else:
        sns.heatmap(df, vmin=mn, vmax=mx, cmap = col, linewidth = 0.05, linecolor = 'blue', annot = True, 
                fmt = annot_fmt, annot_kws={'fontsize':30, 'fontweight': 'bold'})
    plt.title(ttl, fontname="Times New Roman", size=45, fontweight="bold")
    
    # save the base64 strings corresponding to PNG & SVG
    stringIObytes1 = io.BytesIO()
    plt.savefig(stringIObytes1, format='PNG')
    stringIObytes2 = io.BytesIO()
    plt.savefig(stringIObytes2, format='SVG')
    
    if(plot_show =='On'): plt.show() 
    else: 
        plt.savefig(fname)
        plt.close()
    
    # get the base64 strings corresponding to PNG & SVG
    stringIObytes1.seek(0)
    base64PNG = base64.b64encode(stringIObytes1.read()).decode('ascii')
    stringIObytes2.seek(0)
    base64SVG = base64.b64encode(stringIObytes2.read()).decode('ascii')
    
    # make the plotly plots
    fig = px.imshow(df)
    fig = fig.to_json()
    info = json.loads(fig)
    plotlyData = info["data"]
    plotlyLayout = info["layout"]
    
    return base64PNG, base64SVG, plotlyData, plotlyLayout
    
    
######### function to plot bargraph ##########    
    
def plot_bargraph(data, labels, xlab, ylab, ttl, plot_show, fname = './plots/dummy.jpg'):
    """This function plots bargraph of 'data' with 'labels' as bar-labels
 
     Parameters:
     argument1 (array): data for which bargraph to be plot
     argument2 (array): bar-labels
     argument3 (string): label of x-axis
     argument4 (string): label of y-axis
     argument5 (string): plot title
     
     Returns:
     None
 
    """
    
    # pre-process data
    x = range(len(data))
    data = data.to_numpy()
    
    plt.rcParams["figure.figsize"] = (40, 15)
    plt.rcParams.update({'font.size': 30})
    # create the map for colors
    norm = plt.Normalize(vmin = 0, vmax =  max(data))
    cmap = plt.cm.get_cmap('YlGnBu')
    colors = cmap(norm(data))
    
    # plot the bargraph
    plt.bar(x, data, color = colors, edgecolor = 'b', linewidth = 0.5)
    plt.xticks(x, labels, rotation='vertical')
    plt.xlabel(xlab, fontweight="bold")
    plt.ylabel(ylab, fontweight="bold")
    plt.title(ttl, fontname="Times New Roman", size=45, fontweight="bold")
    
    # show the color map    
    sm = plt.cm.ScalarMappable(cmap = 'YlGnBu', norm = norm)
    sm.set_array([])
    plt.colorbar(sm, orientation = 'vertical')
    
    # save the base64 strings corresponding to PNG & SVG
    stringIObytes1 = io.BytesIO()
    plt.savefig(stringIObytes1, format='PNG')
    stringIObytes2 = io.BytesIO()
    plt.savefig(stringIObytes2, format='SVG')
    
    if(plot_show =='On'): plt.show() 
    else: 
        plt.savefig(fname)
        plt.close()
    
    # get the base64 strings corresponding to PNG & SVG
    stringIObytes1.seek(0)
    base64PNG = base64.b64encode(stringIObytes1.read()).decode('ascii')
    stringIObytes2.seek(0)
    base64SVG = base64.b64encode(stringIObytes2.read()).decode('ascii')
    
    # make the plotly plots
    data_ = pd.DataFrame(data, labels)
    fig = px.bar(data_, labels, title=ttl)
    fig = fig.to_json()
    info = json.loads(fig)
    plotlyData = info["data"]
    plotlyLayout = info["layout"]
    
    return base64PNG, base64SVG, plotlyData, plotlyLayout
    
######### function to plot trend ########## 
    
def plot_trend(data, xlab, ylab, ttl, plot_show, fname = './plots/dummy.jpg'):
    """This function plots trend of 'data'
 
     Parameters:
     argument1 (array): data for which trend to be plot
     argument2 (string): label of x-axis
     argument3 (string): label of y-axis
     argument4 (string): plot title
     
     Returns:
     None
 
    """
    
    # set plot parameters 
    plt.rcParams["figure.figsize"] = (40, 15)
    plt.rcParams.update({'font.size': 30})
    plt.style.use('seaborn-darkgrid')
    # plot data points
    plt.plot(data, 'bo', markersize=16)
    # plot trend line
    plt.plot(data, linewidth = 4)
    # label the plot
    plt.xlabel(xlab, fontweight="bold")
    plt.ylabel(ylab, fontweight="bold")
    plt.title(ttl, fontname="Times New Roman", size=45, fontweight="bold")
    
    # save the base64 strings corresponding to PNG & SVG
    stringIObytes1 = io.BytesIO()
    plt.savefig(stringIObytes1, format='PNG')
    stringIObytes2 = io.BytesIO()
    plt.savefig(stringIObytes2, format='SVG')
    
    if(plot_show =='On'): plt.show()
    else: 
        plt.savefig(fname)
        plt.close()
    
    # get the base64 strings corresponding to PNG & SVG
    stringIObytes1.seek(0)
    base64PNG = base64.b64encode(stringIObytes1.read()).decode('ascii')
    stringIObytes2.seek(0)
    base64SVG = base64.b64encode(stringIObytes2.read()).decode('ascii')
    
    # make the plotly plots
    fig = px.line(data, title=ttl)
    fig = fig.to_json()
    info = json.loads(fig)
    plotlyData = info["data"]
    plotlyLayout = info["layout"]
    
    return base64PNG, base64SVG, plotlyData, plotlyLayout
    
    
    