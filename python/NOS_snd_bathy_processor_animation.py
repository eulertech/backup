# -*- coding: utf-8 -*-
def get_ngs_coastline(shapefile=r"E:\HSOFS\Data\CUSP\North_Atlantic.shp",layer="North_Atlantic"):
    from osgeo import ogr
    #shapefile =r'E:\HSOFS\Data\CUSP\North_Atlantic.shp'
    driver = ogr.GetDriverByName("ESRI Shapefile")
    dataSource = driver.Open(shapefile,0)
    layer = dataSource.GetLayer("North_Atlantic")
    points = []
    for feat in layer:
        geom = feat.GetGeometryRef().GetPoints()
        geom.append((np.nan,np.nan))
        points.extend(geom)

    coastline_array = np.asarray(points)
    #plt.figure(num=1,figsize=(5,5),dpi=200)
    #plt.plot(locs[:,0],locs[:,1],'r-')
    return coastline_array

import gzip, glob, os
import csv, matplotlib
import pandas as pd
import numpy as np
import os.path as path
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib import animation
plt.rcParams['animation.ffmpeg_path'] = r'C:\python_libraries\ffmpeg-20161027-bf14393-win64-static\bin\ffmpeg.exe'
basedir = 'E:\\VDATUM\\Model\\NYsndbght02\\DA\\bathy\\NOS_snd'
coastline = r'C:\Matlab_toolbox\surgeLAB\data\coastlines\usa.dat'
latest_cst = get_ngs_coastline(r"E:\HSOFS\Data\CUSP\North_Atlantic.shp","North_Atlantic")
cst = pd.read_csv(coastline,delim_whitespace=True, names = ['Lon','Lat'],na_values = 'NaN')
colormap = plt.cm.jet

animateit = True


gz_list = []
#remove file other than *.gz
for file in os.listdir(basedir):
    if file.endswith(".gz"):
        gz_list.append(file)

startYear = int(gz_list[0].split('.')[0])
endYear = int(gz_list[-1].split('.')[0])+1
norm = matplotlib.colors.Normalize(vmin = startYear, vmax = endYear)
cmap = matplotlib.cm.get_cmap('Set1')
clrs = [[cmap(norm(y))] for y in range(startYear,endYear)]
colnames = ['ID','LON','LAT','DEPTH','YEAR','VDATUM','HDATUM','NGDC']
df_combined = pd.DataFrame(columns = colnames)
n = 0
for file in gz_list:
    print(file)
    year = int(file.split('.')[0])
    f = gzip.open(path.join(basedir,file),'rt')  #'rb' for 2.7
    reader = csv.reader(f)
    df_interm = pd.DataFrame(list(reader))
    df_interm.columns = colnames
    df_interm['LON'] = df_interm['LON'].astype('float')
    df_interm['LAT'] = df_interm['LAT'].astype('float')
    df_interm['DEPTH'] = df_interm['DEPTH'].astype('float')
    df_interm['YEAR'] = df_interm['YEAR'].astype('float')
    if animateit != True :
        fig,ax = plt.subplots(num=n+1,nrows=1,ncols = 1,figsize = (5,5),dpi=300)
        ax.plot(latest_cst[:,0],latest_cst[:,1],linewidth = 0.2, color='gray',label='coastline')
        ax.set_xlim([-76, -70])
        ax.set_ylim([38,43])
        ax.plot(df_interm['LON'], df_interm['LAT'],  linestyle='',marker='o',markersize = 1,mfc=clrs[year-startYear][0][0:3], mec= clrs[year-startYear][0][0:3],label=str(year))
        ax.set_title('Bathymetry footstamp in: ' + str(year))
        picname = os.path.join(basedir,'plots','Z_individual_bathymetry_footstamp_'+str(year)+'.png')
        plt.savefig(picname,dpi=300)
    df_combined = df_combined.append(df_interm)
    n = n+1
# start to make animation

years = df_combined['YEAR'].unique()
if animateit == True:
    print("Start animation!")
    #set up formatting for the movie files
#    Writer = animation.writers['ffmpeg']
    FFWriter = animation.FFMpegWriter()

    fig = plt.figure(num=1)
    ax = plt.axes(xlim=(-76,-70),ylim=(38,43))
    ax.plot(latest_cst[:,0],latest_cst[:,1],linewidth = 0.2, color='gray',label='coastline')
    # update per year , only one line in each frame
    lines = [plt.plot([],[],linestyle='',marker='o',markersize=0.5,mfc='red',mec='red')[0] for _ in range(1)]
    def init():
        for line in lines:
            line.set_data([],[])
        return lines

    def update_line(i):
        #print("animate function")
        year = years[i]
        df_combined.head()
        #print("setting data (%d,%d)"%(len(df_combined[df_combined['YEAR']==year]['LON']),len(df_combined[df_combined['YEAR']==year]['LAT'])))
        lines[0].set_data(df_combined[df_combined['YEAR']==year]['LON'], df_combined[df_combined['YEAR']==year]['LAT'])
        plt.title('Bathymetry FootStamp in '+str((int(year))))
        return lines
    bathy_anim = animation.FuncAnimation(fig,update_line, init_func = init,
                                   frames=len(gz_list),interval=500,blit=True)

    movie_name = os.path.join(basedir,'plots','historical_bathy_footstamp_byYear.mp4')
    bathy_anim.save(movie_name,writer=FFWriter,fps=1)
    print("Animation successfully created at:")
    print(movie_name)
