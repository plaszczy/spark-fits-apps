#INEXLIB
import inlib
import window
import exlib_window as exlib


###################PLOT#########################
#
def plot3D(data,width=700,height=500,pointSize=1.2):
    c3 = inlib.histo_c3d('xyz')
    [c3.fill(row[0],row[1],row[2],1) for row in data]

    #plotter
    plotter = window.gui_plotter(inlib.get_cout(),1,1,0,0,width,height)

    #scen graph plotter
    sgp=plotter.plot_cloud3D(c3)
    sgp.shape.value(inlib.sg_plotter.xyz)
    sgp.shape_automated.value(False)
    sgp.infos_style().visible.value(False)
    sgp.points_style(0).color.value(inlib.colorf_black())
    #sgp.points_style(0).modeling.value(inlib.modeling_points())
    sgp.points_style(0).marker_style.value(inlib.marker_dot)
    sgp.points_style(0).point_size.value(pointSize)
#
    plotter.show()
    plotter.steer()

    del plotter
    del c3

#################################################
def plot3D_colored(data,width=700,height=500,col_index=3,col_minmax=None,client=False):
    
    #///////////////////////////////////
    #/// header ///////////////////
    #///////////////////////////////////
    all_sep = inlib.sg_separator()
  
    camera = inlib.sg_ortho()
    camera.thisown = 0
    camera.position.value(inlib.vec3f(0,0,5))
    camera.height.value(2)
    camera.znear.value(0.1)
    camera.zfar.value(100)
    all_sep.add(camera)

    light = inlib.sg_head_light()
    light.thisown = 0
    light.direction.value(inlib.vec3f(1,-1,-10))
    #light.on.value(False)
    all_sep.add(light)

    layout = inlib.sg_matrix()
    layout.thisown = 0
    #layout.set_rotate(0,1,0,0.785)
    all_sep.add(layout)


    #/// create the scene graph : /////////////////////////////////
    cmap = inlib.SOPI_midas_heat()
    cmap_size = cmap.size()

    sep = inlib.sg_separator()
    m = inlib.sg_matrix()
    m.thisown = 0
    sep.add(m)

    vtxs = inlib.sg_atb_vertices()
    vtxs.thisown = 0
    vtxs.mode.value(inlib.points())
    sep.add(vtxs)

    #color according to redshift
    #look for min/max
    if col_minmax==None:
        xmin=data[0][col_index]
        xmax=data[0][col_index]
        for row in data[1:] :
            if row[col_index]<xmin:
                xmin=row[col_index]
            if row[col_index]>xmax:
                xmax=row[col_index]
        mm=(xmin,xmax)
    else:
        mm=col_minmax

    print("minmax ",mm)
    for row in data:
        color_factor = (float(row[col_index])-mm[0])/(mm[1]-mm[0])
#        icolor = int((1.0-color_factor)*(cmap_size-1))
        icolor = int((color_factor)*(cmap_size-1))
        SOPI_color = cmap.get_color(icolor)  # with midas_heat : icolor 0 is black, size-1 is white.
        r = SOPI_color.r()
        g = SOPI_color.g()
        b = SOPI_color.b()
        a = 1
        #vtxs.add(float(row[0]),float(row[1]),float(row[2]))
        vtxs.add_pos_color(float(row[0]),float(row[1]),float(row[2]),r,g,b,a)

    vtxs.center()

    # plotting/////////////////////////////////////////////

    if not client:
        smgr = exlib.session(inlib.get_cout()) # screen manager
        if smgr.is_valid() == True :
            viewer = exlib.gui_viewer_window(smgr,0,0,width,height)
            if viewer.has_window() == True :
                sep.thisown = 0
                all_sep.add(sep)
                all_sep.thisown = 0
                viewer.scene().add(all_sep);
                viewer.set_scene_camera(camera);
                viewer.set_scene_light(light);
                viewer.set_plane_viewer(False);
                viewer.set_scene_light_on(True);
  
                viewer.hide_main_menu();
                viewer.hide_meta_zone();
                viewer.show_camera_menu();

            viewer.show();
            viewer.steer();
      
            del viewer
        del smgr

    else:
    # client mode
        del all_sep
    
        host = "127.0.0.1"
        port = 50800
        print("try to connect to "+host+" "+str(port)+" ...")
  
        import exlib_offscreen as exlib
        dc = exlib.net_sg_client(inlib.get_cout(),False,True)  #False=quiet, True=warn if receiving unknown protocol.
        if dc.initialize(host,port) == False:
            print("can't connect to "+host+" "+str(port))
            exit()

        if dc.send_string(inlib.sg_s_protocol_clear_static_sg()) == False:
            print("send protocol_clear_static_scene() failed.")
            exit()

        opts = inlib.args()
        opts.add(inlib.sg_s_send_placement(),inlib.sg_s_placement_static())
        if dc.send_sg(sep,opts) == False:
            print("send_sg failed.")
            exit()

        if dc.socket().send_string(inlib.sg_s_protocol_disconnect()) == False:
            print("send protocol_s_disconnect() failed.")
            exit()

        dc.socket().disconnect()
        del dc

        del sep
