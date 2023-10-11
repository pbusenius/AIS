import { Box } from "@mui/material";
import {React } from 'react';
import {Map} from 'react-map-gl';
import {DeckGL, GeoJsonLayer} from 'deck.gl'; 
import 'mapbox-gl/dist/mapbox-gl.css';

import ddpi_data from "../../data/ddpi.geojson";

const MAPBOX_STYLE = "mapbox://styles/mapbox/light-v10";
const MAPBOX_ACCESS_TOKEN = "pk.eyJ1IjoicGJ1c2VuaXVzIiwiYSI6ImNsaTN5bDl1aDExMjAzbGxwdTZzcXR4cGYifQ.NTvbaryPiZwV8n_yNBJq3g";
const NAV_CONTROL_STYLE = {
  position: 'absolute',
  top: 10,
  left: 10
};
const INITIAL_VIEW_STATE = {
  latitude: 20,
  longitude: 0,
  zoom: 2,
  minZoom: 2,
  bearing: 0,
  pitch: 30
};


const DDPI = () => { 
    const layers = [
        new GeoJsonLayer({
            id: 'geojson',
            data: ddpi_data,
            stroked: false,
            filled: true,
            getLineColor: [255, 255, 255],
            pickable: true
          })
      ];
  return (
    <Box>
      <div style={{position:"relative", height:"100vh"}}>
        <DeckGL
            initialViewState={INITIAL_VIEW_STATE}
            controller={true}
            layers={layers}
        >
        <Map
            mapStyle={MAPBOX_STYLE}
            mapboxAccessToken={MAPBOX_ACCESS_TOKEN}
            NavigationControl style={NAV_CONTROL_STYLE} 
        />      
        </DeckGL>
      </div>
    </Box>
  );
};

const DDAPI = () => {        
      const layers = [
        new GeoJsonLayer({
            id: 'geojson',
            data: ddpi_data,
            stroked: false,
            filled: true,
            getLineColor: [255, 255, 255],
            pickable: true
          })
      ];
  return (
    <Box>
      <div style={{position:"relative", height:"100vh"}}>
        <DeckGL
            initialViewState={INITIAL_VIEW_STATE}
            controller={true}
            layers={layers}
            position="relative"
        >
        <Map
            mapStyle={MAPBOX_STYLE}
            mapboxAccessToken={MAPBOX_ACCESS_TOKEN}
            NavigationControl style={NAV_CONTROL_STYLE} 
        />      
        </DeckGL>
      </div>
    </Box>
  );
};

export { DDPI, DDAPI };
