import { Box } from "@mui/material";
import {React } from 'react';
import {Map} from 'react-map-gl';
import {DeckGL, IconLayer } from 'deck.gl'; 
import 'mapbox-gl/dist/mapbox-gl.css';
import adsb_data from "../../assets/airplanes.json";
import airplane_icon from "../../assets/airplane.png";

const UPDATE_TIME = 60;
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

const AIRPLANES = getAirplaneState();


function getAirplaneState() {
    return adsb_data.states.map(state => ({
      callsign: state[1],
      longitude: state[5],
      latitude: state[6],
      velocity: state[9],
      altitude: state[13],
      origin_country: state[2],
      true_track: -state[10],
      last_timestamp: state[4],
      last_position_timestamp: state[3]
    })).filter(function(state) {
      return state.latitude != null &&                                             // remove null latitude 
             state.longitude != null &&                                            // remove null longitude
             state.last_timestamp - state.last_position_timestamp <= UPDATE_TIME;  // remove inactive flights 
    })
  }


const LiveADSB = () => { 
      function getTooltip({object}) {
        return (
          object &&
          `\
          Call Sign: ${object.callsign || ''}
          Country: ${object.origin_country || ''}
          Velocity: ${object.velocity || 0} m/s
          Direction: ${object.true_track || 0}`
        );
      }       
      const layers = [
        new IconLayer({
          id: "airplanes",
          data: AIRPLANES,
          iconAtlas: airplane_icon,
          sizeScale: 25,
          iconMapping: {
              airplane: {
                  x: 0,
                  y: 0,
                  width: 64,
                  height: 64
              }
          },
          pickable: true,
          getPosition: state => [state.longitude, state.latitude],
          getIcon: d => "airplane",
          getAngle: state => 45 + (state.true_track * 180) / Math.PI
        })
      ];
  return (
    <Box>
      <div style={{position:"relative", height:"100vh"}}>
        <DeckGL
            initialViewState={INITIAL_VIEW_STATE}
            controller={true}
            layers={layers}
            getTooltip={getTooltip}
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

const LiveAIS = () => {    
      // useEffect(() => {
      //   let interval = setInterval(() => {
      //     fetch("https://opensky-network.org/api/states/all").then((response) => {
      //       response.json().then((data) => {
      //           console.log(data);
      //       });
      //   });
      //   }, 1000 * UPDATE_TIME);
      //   return () => {
      //     clearInterval(interval);
      //   };
      // }, []);
    
      const layers = [
        new IconLayer({
          id: "airplanes",
          data: AIRPLANES,
          iconAtlas: airplane_icon,
          sizeScale: 25,
          sizeMinPixels: 10,
          iconMapping: {
              airplane: {
                  x: 0,
                  y: 0,
                  width: 64,
                  height: 64
              }
          },
          pickable: true,
          getPosition: state => [state.longitude, state.latitude],
          getIcon: d => "airplane",
          getAngle: state => 45 + (state.true_track * 180) / Math.PI
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

export { LiveAIS, LiveADSB };
