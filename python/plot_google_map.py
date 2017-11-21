def plot_goolge_map(axis, height=640, width=640, scale=2,maptype='terrain',
                    autoAxis=1,**kwargs):
    
#There is a bug with the PNG image interpolation when specified as 'roadmap'    
# coding: utf-8

# # function to plot google map on the current axes using the Google Static Maps API
# 
# # USAGE:
# ```
# plot_goolge_map(ax)
# or:
# lonVec latVec imag = plot_goolge_map(**kwrags)
# 
# % PROPERTIES:
# % all properites are lowercase
# %    Axis           - (axhanle) Axis handle. If not given, gca is used. (LP)
# %    Height (640)   - (height) Height of the image in pixels (max 640)
# %    Width  (640)   - (width) Width of the image in pixels (max 640)
# %    Scale (2)      - (scale) (1/2) Resolution scale factor. Using Scale=2 will
# %                     double the resulotion of the downloaded image (up
# %                     to 1280x1280) and will result in finer rendering,
# %                     but processing time will be longer.
# %    MapType        - (maptype) ('roadmap') Type of map to return. Any of [roadmap, 
# %                     satellite, terrain, hybrid]. See the Google Maps API for
# %                     more information. 
# %    Alpha (1)      - (0-1) Transparency level of the map (0 is fully
# %                     transparent). While the map is always moved to the
# %                     bottom of the plot (i.e. will not hide previously
# %                     drawn items), this can be useful in order to increase
# %                     readability if many colors are plotted 
# %                     (using SCATTER for example).
# %    ShowLabels (1) - (0/1) Controls whether to display city/street textual labels on the map
# %    Language       - (string) A 2 letter ISO 639-1 language code for displaying labels in a 
# %                     local language instead of English (where available).
# %                     For example, for Chinese use:
# %                     plot_google_map('language','zh')
# %                     For the list of codes, see:
# %                     http://en.wikipedia.org/wiki/List_of_ISO_639-1_codes
# %    Marker         - The marker argument is a text string with fields
# %                     conforming to the Google Maps API. The
# %                     following are valid examples:
# %                     '43.0738740,-70.713993' (default midsize orange marker)
# %                     '43.0738740,-70.713993,blue' (midsize blue marker)
# %                     '43.0738740,-70.713993,yellowa' (midsize yellow
# %                     marker with label "A")
# %                     '43.0738740,-70.713993,tinyredb' (tiny red marker
# %                     with label "B")
# %    Refresh (1)    - (0/1) defines whether to automatically refresh the
# %                     map upon zoom/pan action on the figure.
# %    AutoAxis (1)   - (0/1) defines whether to automatically adjust the axis
# %                     of the plot to avoid the map being stretched.
# %                     This will adjust the span to be correct
# %                     according to the shape of the map axes.
# %    FigureResizeUpdate (1) - (0/1) defines whether to automatically refresh the
# %                     map upon resizing the figure. This will ensure map
# %                     isn't stretched after figure resize.
# %    APIKey         - (string) set your own API key which you obtained from Google: 
# %                     http://developers.google.com/maps/documentation/staticmaps/#api_key
# %                     This will enable up to 25,000 map requests per day, 
# %                     compared to a few hundred requests without a key. 
# %                     To set the key, use:
# %                     plot_google_map('APIKey','SomeLongStringObtaindFromGoogle')
# %                     You need to do this only once to set the key.
# %                     To disable the use of a key, use:
# %                     plot_google_map(ax,'APIKey'='')
# %
# % OUTPUT:
# %    h              - Handle to the plotted map
# %
# %    lonVect        - Vector of Longidute coordinates (WGS84) of the image 
# %    latVect        - Vector of Latidute coordinates (WGS84) of the image 
# %    imag           - Image matrix (height,width,3) of the map
# %
# % EXAMPLE - plot a map showing some capitals in Europe:
# %    lat = [48.8708   51.5188   41.9260   40.4312   52.523   37.982];
# %    lon = [2.4131    -0.1300    12.4951   -3.6788    13.415   23.715];
# %    fig,ax = plt.subplots(111,figsize=(5,5),dpi=300)
# %    ax.plot(lon,lat,'r.',markdersize=10,zorder=1)
# %    lonVect,LatVect,imagMatrix = plot_google_map(ax,width=640,height=640)
# 
# % References:
# %  http://www.mathworks.com/matlabcentral/fileexchange/24113
# %  http://www.maptiler.org/google-maps-coordinates-tile-bounds-projection/
# %  http://developers.google.com/maps/documentation/staticmaps/
# %
# % Acknowledgements:
# %  Val Schmidt for his submission of get_google_map.m
# %
# % Author:
# %  Dr. Liang Kuang
#  2016-11-22
# ```
#     
    import numpy as np
    import matplotlib.pyplot as plt
    import wget,os
    from scipy.interpolate import RectBivariateSpline,interp2d
    
    def num2str(a,precision = 0):
        fmt = "%."+str(precision) +"f"
        return fmt % a
    def updateAxis(axHandle,curAxis):
        if curAxis[2] < -85:
            curAxis[2] = -85
        if curAxis[3] > 85:
            curAxis[3] = 85
        if curAxis[0] < -180:
            curAxis[0] = -180
        if curAxis[0] > 180:
            curAxis[0] = 0
        if curAxis[1] < -180:
            curAxis[1] = 0
        if curAxis[1] > 180:
            curAxis[1] = 0
            
        if curAxis == [0,1,0,1]:
            axHandle.set_xlim(-200,200)
            axHandle.set_ylim(-85,85)
        curAxis = [axHandle.get_xlim()[0], axHandle.get_xlim()[1], axHandle.get_ylim()[0],axHandle.get_ylim()[1]]            
        return curAxis
        
    def updateGoogleAR(width,height,curAxis):
        AR =  float((curAxis[3] - curAxis[2])/(curAxis[1] - curAxis[0]))
        height = width * AR
            
        return (width,height)
    
    def computeZoomLevel(curAxis,width,height):
        import numpy as np
        [xExtent, yExtent] = latLonToMeters(curAxis[2::],curAxis[0:2])
        minResX = np.diff(xExtent) / width
        minResY = np.diff(yExtent) / height
        minRes = max([minResX, minResY])
        tileSize = 256
        initialResolution = 2 * np.pi * 6378137 / tileSize  # 156543.0339280462 for tileSize 256 pixels
        # calculate zoom level for current axis limits
        zoomlevel = np.floor(np.log2(initialResolution/minRes))
   
        return zoomlevel
        
    def EnforceZoom(zoomlevel):
        # Enforce valid zoom levels
        if zoomlevel < 0:
            zoomlevel = 0
        if zoomlevel > 19:
            zoomlevel = 19
        return zoomlevel
    # construct query URL      
    def generate_map_url(lon,lat,zoomlevel,scale,width,height,maptype,**kwargs):
        alphaData = kwargs.get('alphaData',1)
        autoRefresh = kwargs.get('autoRefresh',0)
        figureResizeUpdate = kwargs.get('figureResizeUpdate',0)
        language = kwargs.get('language','')
        showLabels = kwargs.get('showLabels','')    
        apiKey = kwargs.get('apiKey','')
        markeridx = 1;
        markerlist = [];
                    
        preamble = 'http://maps.googleapis.com/maps/api/staticmap'
        location = '?center=' + num2str(lat,10) + ',' + num2str(lon,10)
        zoomStr = '&zoom=' + num2str(zoomlevel)
        sizeStr = '&scale=' + num2str(scale) + '&size=' + num2str(width) + 'x' + num2str(height)
        maptypeStr = '&maptype=' + maptype
        if apikey != '' :
            keyStr = '&key=' + apiKey
        else:
            keyStr = ''
        #googlemapAPIKey = 'AIzaSyC2kYdVHSzqSFn9FYGiX4oNWDe6MqxC9uM'

        markers = '&markers='
        for idx in range(len(markerlist)):
            if idx < len(markerlist):
                markers = markers + markerlist[idx] + '%7C'
            else:
                markers = markers + markerlist[idx]        
        if showLabels == 0 :
            labelsStr = '&style=feature:all|element:labels|visibility:off'
        else:
            labelsStr = ''
        
        if language != '' :
            languageStr = '&language=' + language
        else:
            languageStr = ''
        if maptype in ['satellite','hybrid','terrain']:
            filename = 'tmp.jpg'
            fmt = '&format=jpg'
            convertNeeded = 0
        else:
            filename = 'tmp.png'
            fmt = '&format=png'
            convertNeeded = 1;
        sensor = '&sensor=false';
        url = preamble +location + zoomStr +sizeStr+ maptypeStr +fmt+ markers+ labelsStr +languageStr +sensor+ keyStr
        
        return (url,filename,convertNeeded)
        
    def metersToLatLon(x,y):
    # Converts XY point from Spherical Mercator EPSG:900913 to lat/lon in WGS84 Datum
        x = np.asarray(x)
        y = np.asarray(y)
        originShift = 2 * np.pi * 6378137 / 2.0; # 20037508.342789244
        lon = (x / originShift) * 180;
        lat = (y / originShift) * 180;
        lat = 180 / np.pi * (2 * np.arctan( np.exp( lat * np.pi / 180)) - np.pi / 2);
        return (lon,lat)
    def latLonToMeters(lat,lon):    
    # Converts given lat/lon in WGS84 Datum to XY in Spherical Mercator EPSG:900913"
        lon = np.asarray(lon)
        lat = np.asarray(lat)
        pi = np.pi
        originShift = 2 * pi * 6378137 / 2.0  # 20037508.342789244
        x = lon * originShift / 180
        y = np.log(np.tan((90 + lat) * pi / 360 )) / (pi / 180)
        y = y * originShift / 180
        return (x,y)
    
    
    # In[156]:
    
    def myTurboInterp2(X=None, Y=None, Z=None, XI=None, YI=None):
        # An extremely fast nearest neighbour 2D interpolation, assuming both input
        # and output grids consist only of squares, meaning:
        # - uniform X for each column
        # - uniform Y for each row
        XI = XI[0,:]
        X = X[0,:]
        YI = YI[:,0]
        Y = Y[:,0]
        ZI = np.nan * np.ones(Z.shape)
    
        xiPos = np.nan * np.ones(XI.shape)
        xLen = max(X.shape)
        yiPos = np.nan * np.ones(YI.shape)
        yLen = max(Y.shape)
        # find x conversion
        xPos = 0
        for idx in range(len(xiPos)):
            if XI[idx] >= X[0] and XI[idx] <= X[-1]:
                while xPos < xLen -1 and X[xPos + 1] < XI[idx]:
                    xPos = xPos + 1
                
                diffs = abs(X[xPos:xPos + 2] - XI[idx])
                
                if diffs[0] < diffs[1]:
                    xiPos[idx] = xPos
                else:
                    xiPos[idx] = xPos + 1
                    
        # find y conversion
        yPos = 0
        for idx in range(len(yiPos)):
            if YI[idx] <= Y[0] and YI[idx] >= Y[-1]:
                while yPos < yLen -1 and Y[yPos + 1] > YI[idx]:
                    yPos = yPos + 1
                diffs = abs(Y[yPos:yPos + 2] - YI[idx])
                if diffs[0] < diffs[1]:
                    yiPos[idx] = yPos
                else:
                    yiPos[idx] = yPos + 1
        print(yiPos.shape)
        print(xiPos.shape)
        xiPos = np.int32(xiPos)
        yiPos = np.int32(yiPos)
        
        for n in range(len(yiPos)):
            for m in range(len(xiPos)):
                ZI[m,n,:] = Z[xiPos[m],yiPos[n],:]
        
        return ZI
    
    #Default parameters
    #axHandle = plt.gca()
    axHandle = axis

    global apikey
    if 'apikey' not in globals():
        apikey = ''
    print('apikey = %s'%apikey)
    #curAxis = plt.axes(axHandle) #python
 
    # Enforce latitude constraints of EPSG:900913
    curAxis = [axHandle.get_xlim()[0], axHandle.get_xlim()[1], axHandle.get_ylim()[0],axHandle.get_ylim()[1]]
    print(curAxis)
    # update width and height for same aspect ratio as curAxis
    #width,height = updateGoogleAR(width, height,curAxis)
    if height > 640:
        height = 640
    
    if width > 640:
        width = 640
        
    if autoAxis:
    # adjust current axis limit to avoid strectched maps
        [xExtent,yExtent] = latLonToMeters(curAxis[2:], curAxis[0:2] )
        xExtent = np.diff(xExtent); # just the size of the span
        yExtent = np.diff(yExtent); 
        # get axes aspect ratio
        aspect_ratio = (curAxis[3]-curAxis[2]) / (curAxis[1]-curAxis[0])
        print('xExtent %f, yExtent %f, AR %f' %(xExtent,yExtent, aspect_ratio))
        print('A %f'%xExtent*aspect_ratio)
        
        if xExtent*aspect_ratio > yExtent :        
            centerX = np.mean(curAxis[0:2]);
            centerY = np.mean(curAxis[2:]);
            spanX = (curAxis[1]-curAxis[0])/2;
            spanY = (curAxis[3]-curAxis[2])/2;
           
            # enlarge the Y extent
            spanY = spanY*xExtent*aspect_ratio/yExtent; # new span
            if spanY > 85:
                spanX = spanX * 85 / spanY;
                spanY = spanY * 85 / spanY;
            
            curAxis[0] = centerX-spanX;
            curAxis[1] = centerX+spanX;
            curAxis[2] = centerY-spanY;
            curAxis[3] = centerY+spanY;
        elif yExtent > xExtent*aspect_ratio:
            
            centerX = np.mean(curAxis[0:2]);
            centerY = np.mean(curAxis[2:]);
            spanX = (curAxis[1]-curAxis[0])/2;
            spanY = (curAxis[3]-curAxis[2])/2;
            # enlarge the X extent
            spanX = spanX*yExtent/(xExtent*aspect_ratio); # new span
            if spanX > 180 :
                spanY = spanY * 180 / spanX;
                spanX = spanX * 180 / spanX;
        
            
            curAxis[0] = centerX-spanX;
            curAxis[1] = centerX+spanX;
            curAxis[2] = centerY-spanY;
            curAxis[3] = centerY+spanY;
         
        # Enforce Latitude constraints of EPSG:900913
        if curAxis[2] < -85 :
            curAxis[2:] = curAxis[2:] + (-85 - curAxis[2]);
        
        if curAxis[3] > 85 :
            curAxis[2:] = curAxis[2:] + (85 - curAxis[3]);
    curAxis = updateAxis(axHandle,curAxis)
    print(curAxis)

        
    zoomlevel = computeZoomLevel(curAxis,width,height)
    zoomlevel = EnforceZoom(zoomlevel)            
    print(zoomlevel)
     # Calculate center coordinate in WGS1984
    lat = (curAxis[2] + curAxis[3]) / 2
    lon = (curAxis[0] + curAxis[1]) / 2
       

    url,filename,convertNeeded = generate_map_url(lon,lat,zoomlevel,scale,width,height,maptype,**kwargs)
 
    #Get the image
    try:
        os.remove(filename)
    except:
        pass        
    try:
        fname = wget.download(url)
        os.rename(fname,filename)
    except:
        print("Unable to download map from Google Servers." + url)
    #image_original = plt.imread(filename)
    print(filename)    
    from PIL import Image
    im = Image.open(filename)
    indexed = np.asarray(im) # Convert to NumPy array to easier access
    #Get the color palette for PNG
    palette = im.getpalette()
    max_val = np.float32(np.iinfo(indexed.dtype).max)    
    width = indexed.shape[1]
    height = indexed.shape[0]
    print('width is %d ,height is %d'%(width,height))
    tileSize = 256
    initialResolution = 2 * np.pi * 6378137 / tileSize  # 156543.0339280462 for tileSize 256 pixels
    # calculate a meshgrid of pixel cooridinates in EPSG:900913
    centerPixelY = round(height/2);
    centerPixelX = round(width/2);
    [centerX,centerY] = latLonToMeters(lat, lon ); # center coordinates in EPSG:900913
    curResolution = initialResolution / 2**zoomlevel/scale; # meters/pixel (EPSG:900913)
    xVec = centerX + (np.arange(1,width+1)-centerPixelX) * curResolution; # x vector
    yVec = centerY + (np.arange(height+1,1,-1)-centerPixelY) * curResolution; # y vector
    #yVec = centerY + (np.arange(1,height+1)-centerPixelY) * curResolution; # y vector
    [xMesh,yMesh] = np.meshgrid(xVec,yVec); # construct meshgrid 
    [xVec_WGS, yVec_WGS]=metersToLatLon(xVec,yVec)
    #Convert meshgrid to WGS1984
    [lonMesh,latMesh] = metersToLatLon(xMesh,yMesh);
    # We now want to convert the image from a colormap image with an uneven
    # mesh grid, into an RGB truecolor image with a uniform grid.
    # This would enable displaying it with IMAGE, instead of PCOLOR.
    # Advantages are:
    # 1) faster rendering
    # 2) makes it possible to display together with other colormap annotations (PCOLOR, SCATTER etc.)
    
    # Convert image from colormap type to RGB truecolor (if PNG is used)
    if convertNeeded:      
        #Create a color map matrix
        num_colors = len(palette)/3
        Mcolor = np.asarray(palette).reshape(num_colors,3)/max_val        
        imag = np.zeros([height, width, 3])
        for idx in range(3):
            imag[:,:,idx] = np.reshape(Mcolor.ravel()[indexed.ravel()+idx*Mcolor.shape[0]],(height,width))   
    else:
        imag = indexed / 255

    # Next, project the data into a uniform WGS1984 grid
    sizeFactor = 1; # factoring of new image
    uniHeight = round(height*sizeFactor);
    uniWidth = round(width*sizeFactor);
    latVect = np.linspace(latMesh[0,0],latMesh[-1,0],uniHeight);
    lonVect = np.linspace(lonMesh[0,0],lonMesh[0,-1],uniWidth);
    [uniLonMesh,uniLatMesh] = np.meshgrid(lonVect,latVect);
    uniImag = np.zeros([int(uniHeight),int(uniWidth),3]);
    
    uniImag = myTurboInterp2(lonMesh,latMesh,imag,uniLonMesh,uniLatMesh)
    imageAxis = [min(lonVect),max(lonVect),min(latVect),max(latVect)]
    plt.imshow(uniImag,zorder=0,extent=imageAxis)
    plt.axes(curAxis)   
    return (lonVect,latVect,uniImag)
if __name__ == "__main__":
    import matplotlib.pyplot as plt
    import numpy as np
    from mpl_toolkits.axes_grid1 import make_axes_locatable
    plt.close('all')
    fig,ax = plt.subplots(nrows=1,ncols=1,figsize=(5,5))
    [x,y]=np.meshgrid(np.arange(-73,-70,0.2),np.arange(40,43,0.2))
    pc = ax.pcolor(x,y,np.random.random(x.shape))
    plt.colorbar(pc,fraction=0.046, pad=0.04)
    ax.scatter([-73,-72.5555],[39.4,41.00000],zorder=1)
    lonVect,latVect,uniImag = plot_goolge_map(ax)
    plt.show()
    ax.set_title('test plot google map')
    plt.savefig('test_plot.png',bbox_inches='tight')
    
