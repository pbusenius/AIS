import { useState } from "react";
import Live from "./scenes/live";
import DDPI from "./scenes/ddpi";
import Kritis from "./scenes/kritis";
import VOI from "./scenes/voi";
import ROI from "./scenes/roi";

function App() {
  return (
    <div className="app">
      <Sidebar isSidebar={isSidebar}/>
      <main className="content">
        <Topbar setIsSidebar={setIsSidebar}/>
        <Routes>
          <Route path="/live" element={<Live/>}/>
          <Route path="/kritis" element={<Kritis/>}/>
          <Route path="/ddpi" element={<DDPI/>}/>
          <Route path="/roi" element={<ROI/>}/>
          <Route path="/voi" element={<VOI/>}/>
        </Routes>
      </main>
    </div>
  );
}

export default App;
