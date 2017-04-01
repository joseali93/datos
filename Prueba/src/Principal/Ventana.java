package Principal;

import java.awt.EventQueue;

import javax.swing.JFrame;
import java.awt.BorderLayout;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JTextField;
import javax.swing.JButton;
import java.awt.event.ActionListener;
import java.awt.event.ActionEvent;
import com.socrata.api.HttpLowLevel;
import com.socrata.api.Soda2Consumer;
import com.socrata.api.SodaImporter;
import com.socrata.exceptions.LongRunningQueryException;
import com.socrata.exceptions.SodaError;
import com.socrata.model.importer.DatasetInfo;
import com.socrata.model.importer.LicenseInfo;
import com.socrata.model.search.SearchClause;
import com.socrata.model.soql.SoqlQuery;
import com.sun.jersey.api.client.ClientResponse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.JFrame;
import javax.swing.JTextField;

import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;

import com.google.gson.Gson;

public class Ventana {

	JFrame frmConectarDatos;
	JTextField listadoCodigos;
	private JTextField directorio;
	

	/**
	 * Launch the application.
	 */


	/**
	 * Create the application.
	 */
	public Ventana() {
		initialize();
	}

	/**
	 * Initialize the contents of the frame.
	 */
	private void initialize() {
		List<String> structureOne = Arrays.asList(
				
				"codigo_contrato", 
				"codigo_del_proyecto", 
				"disponibilidades_presupuestales", 
				"evento", 
				"fecha_inicio", 
				"fecha_suscripcion", 
				"identificacion_contratista", 
				"identificacion_interventor", 
				"no_contrato_interventor", 
				"nombre_contratista", 
				"nombre_del_interventor", 
				"nombre_del_proyecto", 
				"objeto_del_contrato", 
				"plazo_estimado", 
				"proceso_de_contratacion", 
				"registros_presupuestales", 
				"sector_del_proyecto", 
				"sujeto_de_control", 
				"tipo_de_registro", 
				"tipo_interventor", 
				"tipologia", 
				"valor_contrato", 
				"valor_del_proyecto", 
				"valor_ejecutado_del_proyecto");
		List<String> structureTwo= Arrays.asList(
				
				 "c_digo_contrato",
				 "c_digo_del_proyecto",
				 "disponibilidades_presupuestales",
				 "evento",
				 "fecha_inicio",
				 "fecha_suscripci_n",
				 "identificaci_n_contratista",
				 "identificaci_ninterventor",
				 "no_contrato_interventor",
				 "nombre_del_interventor",
				 "nombre_delproyecto",
				 "nombrecontratista",
				 "objeto_del_contrato",
				 "plazo_estimado",
				 "proceso_de_contrataci_n",
				 "registros_presupuestales",
				 "sector_del_proyecto",
				 "sujeto_de_control",
				 "tipo_de_registro",
				 "tipo_interventor",
				 "tipolog_a",
				 "valor_contrato",
				 "valor_delproyecto",
				 "valor_ejecutadodel_proyecto");
		Collections.sort(structureOne);
		Collections.sort(structureTwo);
		frmConectarDatos = new JFrame();
		frmConectarDatos.setTitle("CONECTAR DATOS");
		frmConectarDatos.setBounds(100, 100, 566, 275);
		frmConectarDatos.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		frmConectarDatos.getContentPane().setLayout(null);
		
		JLabel lblCodigos = new JLabel("CODIGOS:");
		lblCodigos.setBounds(53, 21, 80, 29);
		frmConectarDatos.getContentPane().add(lblCodigos);
		
		listadoCodigos = new JTextField();
		listadoCodigos.setBounds(135, 25, 320, 20);
		frmConectarDatos.getContentPane().add(listadoCodigos);
		listadoCodigos.setColumns(10);
		
		JButton botonTraer = new JButton("TRAER");
		botonTraer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent e) {
				Soda2Consumer consumer = Soda2Consumer.newConsumer("https://www.datos.gov.co");
				ClientResponse response = null;
				boolean i = true;
				String payload = "";
				String ruta = directorio.getText();
				String Tokens = null;
				borrarArchivo(ruta);
				try {
						StringTokenizer tokenizer = new StringTokenizer(listadoCodigos.getText(), ",");
					    while (tokenizer.hasMoreTokens()) {
					    	
					        String token = tokenizer.nextToken();
					        Tokens = token;
					        response = consumer.query(token,
								    HttpLowLevel.JSON_TYPE,
								    "SELECT * LIMIT 20000");
							payload = response.getEntity(String.class);
							System.out.println(payload);
							PrintWriter writer = new PrintWriter(directorio.getText()+token+".txt", "UTF-8");
							writer.println(payload);
							writer.close();
							List keys1 = getKeysFromJson(directorio.getText()+token+".txt");
							List<String> al = new ArrayList<>();
							Set<String> hs = new HashSet<>();
							hs.addAll(keys1);
							al.clear();
							al.addAll(hs);
							Collections.sort(al);
							if(linearIn(al,structureOne) || linearIn(al,structureTwo)){
								 i = true;
								continue;
							}else{
								 i = false;
								break;
							}	
					    }
					    if(i){
					    	mongoDB insertar = new mongoDB();
					    	insertar.InsertarMongo(ruta);
					    	insertar.InsertarMongoTokens(Tokens);

					    	JOptionPane.showMessageDialog(null, "Datos traidos exitosamente!");
							 System.exit(0);
					    }else{
					    	JOptionPane.showMessageDialog(null, "Verifique los ID'S ya que alguno no posee la estructura", "Error", JOptionPane.ERROR_MESSAGE);
					    	System.exit(0);
					    }
					    
						 
						

				} catch (LongRunningQueryException | SodaError e1) {
					JOptionPane.showMessageDialog(null, "Verifique los ID'S"+e1, "Error", JOptionPane.ERROR_MESSAGE);
					System.exit(0);
					e1.printStackTrace();
				} catch (FileNotFoundException e1) {
					JOptionPane.showMessageDialog(null, "Verifique los ID'S"+e1, "Error", JOptionPane.ERROR_MESSAGE);
					System.exit(0);
					e1.printStackTrace();
				} catch (UnsupportedEncodingException e1) {
					JOptionPane.showMessageDialog(null, "Verifique los ID'S"+e1, "Error", JOptionPane.ERROR_MESSAGE);
					System.exit(0);
					e1.printStackTrace();
				} catch (Exception e1) {
					JOptionPane.showMessageDialog(null, "Verifique los ID'S"+e1, "Error", JOptionPane.ERROR_MESSAGE);
					System.exit(0);
					e1.printStackTrace();
				}
					
				
			}
		});
		botonTraer.setBounds(240, 204, 89, 23);
		frmConectarDatos.getContentPane().add(botonTraer);
		
		JLabel lblInserteLosIds = new JLabel("INSERTE LOS ID'S SEPARADOS POR COMA SIN ESPACIOS EJ:");
		lblInserteLosIds.setBounds(121, 43, 406, 48);
		frmConectarDatos.getContentPane().add(lblInserteLosIds);
		
		JLabel lblQsrcbk = new JLabel("qsrc-b3k4,j3bg-66aw,whvw-q2qr");
		lblQsrcbk.setBounds(214, 89, 276, 29);
		frmConectarDatos.getContentPane().add(lblQsrcbk);
		
		JLabel lblDirectorio = new JLabel("DIRECTORIO:");
		lblDirectorio.setBounds(51, 150, 82, 14);
		frmConectarDatos.getContentPane().add(lblDirectorio);
		
		directorio = new JTextField();
		directorio.setBounds(143, 147, 312, 20);
		frmConectarDatos.getContentPane().add(directorio);
		directorio.setColumns(10);
		
		JLabel lblInserteUnDirectorio = new JLabel("INSERTE UN DIRECTORIO EXISTENTE.");
		lblInserteUnDirectorio.setBounds(191, 179, 227, 14);
		frmConectarDatos.getContentPane().add(lblInserteUnDirectorio);
	}
	public static boolean linearIn(List<String> al, List<String>estructure) {
		
	   return Arrays.asList(al).containsAll(Arrays.asList(estructure));
}


static List getKeysFromJson(String fileName) throws Exception
  {
    Object things = new Gson().fromJson(new FileReader(fileName), Object.class);
    
    List keys = new ArrayList();
    
    collectAllTheKeys(keys, things);
    return keys;
  }

  static void collectAllTheKeys(List keys, Object o)
  {
    Collection values = null;
    if (o instanceof Map)
    {
      Map map = (Map) o;
      keys.addAll(map.keySet()); // collect keys at current level in hierarchy
      values = map.values();
    }
    else if (o instanceof Collection)
      values = (Collection) o;
    else // nothing further to collect keys from
      return;

    for (Object value : values)
      collectAllTheKeys(keys, value);
  }
  private static void borrarArchivo(String direccion) {
      File directorio = new File(direccion);
      File f;
      if (directorio.isDirectory()) {
          String[] files = directorio.list();
          if (files.length > 0) {
              System.out.println(" Directorio vacio: " + direccion);
              for (String archivo : files) {
                  System.out.println(archivo);
                  f = new File(direccion + File.separator + archivo);
                      f.delete();
                      f.deleteOnExit();
              }
          }
      }
  }
}
