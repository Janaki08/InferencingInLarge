import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as pltlib
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2TkAgg
import Tkinter as Tk
from matplotlib.figure import Figure
import Tkinter
from Tkinter import *





class App_Window(Tkinter.Tk):
    def __init__(self,parent):
        Tkinter.Tk.__init__(self,parent)
        self.parent=parent
        self.initialize()


    def initialize(self):
        self.grid()
        self.title("Convergence of Scores")
        self.canvasFig=pltlib.figure(1)
        Fig=matplotlib.figure.Figure(figsize=(5,4), dpi=100)
        FigSubPlot=Fig.add_subplot(111)
        x=[]
        y=[]
        y1=[]
        y2=[]
        self.job=[]
        self.X=[]
        self.allTemps=[]
        self.setPointArray=[]
        self.dampPos=[]
        self.line1,self.line2,self.line3,=FigSubPlot.plot(x,y,'r-',x,y1,'b--',x,y2,'g-')
        self.canvas=matplotlib.backends.backend_tkagg.FigureCanvasTkAgg(Fig,master=self)
        self.canvas.show()
        self.canvas.get_tk_widget().grid(column=5,row=0, sticky=E )
        self.canvas._tkcanvas.grid(column=3,row=0, sticky=E ,rowspan=9, padx=50)
        self.resizable(True,True)
        self.grid_columnconfigure(0,weight=1)
        self.update()

    def refreshFigure(self,x,y,setPoint,z):
        self.line1.set_data(x,y)
        self.line2.set_data(x,setPoint)
        self.line3.set_data(x,z)
        ax = self.canvas.figure.axes[0]
        ax.set_xlim(x.min(), x.max())
        k=max(y.max(),setPoint.max())
        ax.set_ylim(30, 44)
        self.bulbNumberLabel.config(text='No of Bulbs= %d' %self.model.var.numberOfBulbs)
        self.humanNumberLabel.config(text='No of Humans= %d' %self.model.var.numberOfHumans)        
        self.canvas.draw()




if __name__ == "__main__":
    MainWindow = App_Window(None)
    MainWindow.mainloop()
