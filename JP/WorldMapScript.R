library(ggplot2)
library(maps)
library(AACloudTools)

gpsData <- SqlToDf("select callref, imo, ping_arrdate, saildate, latitude, longitude
                   from mar_commoditysea.crudeoil_combineddata
                   where source in ('A', 'O')
                   and callref in (
                   select distinct callid
                   from mar_commoditysea.crudeoil_combineddata
                   where source in ('M')
                   and countryname in ('United States of America', 'China', 'India', 'Spain', 'France', 'Japan', 'Germany', 'Netherlands', 'Korea (South)', 'Italy')
                   and ping_arrdate > '2017-07-01');")

mapWorld <- borders("world", colour="gray25", fill="black")


portData <- AACloudTools::SqlToDf("select distinct portname, latitude, longitude
                                  from ra.tblcombports
                                  where country_name in ('United States of America', 'China', 'India', 'Spain', 'France', 'Japan', 'Germany', 'Netherlands', 'Korea (South)', 'Italy')
                                  and latitude is not null;")

ggplot() + 
  theme(axis.line=element_blank(),axis.text.x=element_blank(),
        axis.text.y=element_blank(),axis.ticks=element_blank(),
        axis.title.x=element_blank(),
        axis.title.y=element_blank(), panel.background = element_rect(fill = 'black'), panel.grid.major = element_blank(), panel.grid.minor = element_blank()) +
  mapWorld + 
  geom_point(data = gpsData, aes(x = longitude, y = latitude, color = imo), size = .05, color = "yellow", 
             alpha = .01) + 
  theme(legend.position = "none")


ggsave("WorldMap_v3.png")

