"""
PHASE 10: Python Dashboard (Enhanced Tkinter Version)
======================================================

Professional desktop dashboard with improved UI/UX.

Features:
- Modern design with gradient header
- Properly scaled images
- Scrollable canvas for large images
- Professional color scheme
- Responsive layout
"""

import tkinter as tk
from tkinter import ttk, Canvas, Scrollbar
from PIL import Image, ImageTk
import duckdb
from pathlib import Path
import sys

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from src.config import AGGREGATED_DIR, FIGURES_DIR


class NYCDashboard:
    """
    NYC Congestion Pricing Audit Dashboard - Enhanced Version
    """
    
    def __init__(self, root):
        self.root = root
        self.root.title("NYC Congestion Pricing Audit 2025 - Interactive Dashboard")
        
        # Set window size and make it resizable
        screen_width = root.winfo_screenwidth()
        screen_height = root.winfo_screenheight()
        window_width = int(screen_width * 0.85)
        window_height = int(screen_height * 0.85)
        
        # Center the window
        x = (screen_width - window_width) // 2
        y = (screen_height - window_height) // 2
        
        self.root.geometry(f"{window_width}x{window_height}+{x}+{y}")
        self.root.minsize(1200, 700)
        
        # Configure style
        self.setup_styles()
        
        # Create main header
        self.create_header()
        
        # Create notebook (tabs)
        self.notebook = ttk.Notebook(root, style='Custom.TNotebook')
        self.notebook.pack(fill='both', expand=True, padx=15, pady=10)
        
        # Create tabs
        self.create_overview_tab()
        self.create_time_series_tab()
        self.create_revenue_tab()
        self.create_zone_tab()
        self.create_leakage_tab()
        
        # Create footer
        self.create_footer()
    
    def setup_styles(self):
        """Setup custom styles for the dashboard"""
        style = ttk.Style()
        style.theme_use('clam')
        
        # Configure notebook style
        style.configure('Custom.TNotebook', background='#f5f5f5', borderwidth=0)
        style.configure('Custom.TNotebook.Tab', 
                       padding=[20, 10], 
                       font=('Arial', 11, 'bold'),
                       background='#e0e0e0')
        style.map('Custom.TNotebook.Tab',
                 background=[('selected', '#1f77b4')],
                 foreground=[('selected', 'white')])
    
    def create_header(self):
        """Create professional header with gradient effect"""
        header_frame = tk.Frame(self.root, bg='#1f77b4', height=100)
        header_frame.pack(fill='x')
        header_frame.pack_propagate(False)
        
        # Title
        title_label = tk.Label(
            header_frame,
            text="🚕 NYC Congestion Pricing Audit 2025",
            font=('Arial', 28, 'bold'),
            bg='#1f77b4',
            fg='white'
        )
        title_label.pack(pady=15)
        
        # Subtitle
        subtitle_label = tk.Label(
            header_frame,
            text="Analysis of the Congestion Relief Zone Toll Effectiveness",
            font=('Arial', 12),
            bg='#1f77b4',
            fg='#e0e0e0'
        )
        subtitle_label.pack()
    
    def create_footer(self):
        """Create footer with info"""
        footer = tk.Frame(self.root, bg='#2c3e50', height=40)
        footer.pack(fill='x', side='bottom')
        footer.pack_propagate(False)
        
        footer_label = tk.Label(
            footer,
            text="Data Source: NYC TLC Trip Records | Processing: DuckDB | Visualization: Matplotlib",
            font=('Arial', 9),
            bg='#2c3e50',
            fg='white'
        )
        footer_label.pack(pady=10)
    
    def create_overview_tab(self):
        """Create overview tab with enhanced metrics"""
        tab = ttk.Frame(self.notebook, style='Custom.TFrame')
        self.notebook.add(tab, text="📊 Overview")
        
        # Configure tab background
        tab.configure(style='Custom.TFrame')
        
        # Main container
        container = tk.Frame(tab, bg='white')
        container.pack(fill='both', expand=True, padx=20, pady=20)
        
        # Title
        title = tk.Label(
            container, 
            text="Key Performance Metrics", 
            font=('Arial', 22, 'bold'),
            bg='white',
            fg='#2c3e50'
        )
        title.pack(pady=(0, 30))
        
        # Load statistics
        try:
            con = duckdb.connect()
            zone_file = AGGREGATED_DIR / "trips_by_zone_category.parquet"
            
            if zone_file.exists():
                stats = con.execute(f"""
                    SELECT 
                        SUM(trip_count) as total_trips,
                        SUM(total_congestion_collected) as total_revenue,
                        SUM(trips_with_surcharge) as trips_with,
                        SUM(trips_without_surcharge) as trips_without
                    FROM read_parquet('{zone_file}')
                    WHERE after_congestion_start = 1
                """).fetchone()
                
                total_trips = stats[0] if stats[0] else 0
                total_revenue = stats[1] if stats[1] else 0
                trips_with = stats[2] if stats[2] else 0
                trips_without = stats[3] if stats[3] else 0
                
                compliance_rate = (trips_with / (trips_with + trips_without) * 100) if (trips_with + trips_without) > 0 else 0
                leakage_rate = 100 - compliance_rate
                
                # Create metrics grid
                metrics_frame = tk.Frame(container, bg='white')
                metrics_frame.pack(pady=20, fill='both', expand=True)
                
                # Configure grid
                for i in range(2):
                    metrics_frame.grid_rowconfigure(i, weight=1)
                    metrics_frame.grid_columnconfigure(i, weight=1)
                
                # Metric cards with colors
                self.create_enhanced_metric_card(
                    metrics_frame, 
                    "Total Trips Analyzed", 
                    f"{total_trips:,.0f}",
                    "#3498db",
                    0, 0
                )
                self.create_enhanced_metric_card(
                    metrics_frame, 
                    "Congestion Revenue", 
                    f"${total_revenue:,.2f}",
                    "#2ecc71",
                    0, 1
                )
                self.create_enhanced_metric_card(
                    metrics_frame, 
                    "Compliance Rate", 
                    f"{compliance_rate:.1f}%",
                    "#9b59b6",
                    1, 0
                )
                self.create_enhanced_metric_card(
                    metrics_frame, 
                    "Leakage Rate", 
                    f"{leakage_rate:.1f}%",
                    "#e74c3c",
                    1, 1
                )
                
            con.close()
            
        except Exception as e:
            error_label = tk.Label(
                container, 
                text=f"Error loading statistics: {e}", 
                fg='red',
                bg='white',
                font=('Arial', 12)
            )
            error_label.pack(pady=20)
    
    def create_enhanced_metric_card(self, parent, title, value, color, row, col):
        """Create an enhanced metric card with color"""
        card = tk.Frame(parent, bg=color, relief='raised', borderwidth=0)
        card.grid(row=row, column=col, padx=15, pady=15, sticky='nsew', ipadx=20, ipady=20)
        
        # Add shadow effect with another frame
        shadow = tk.Frame(card, bg='#bdc3c7', relief='flat')
        shadow.place(x=5, y=5, relwidth=1, relheight=1)
        shadow.lower()
        
        title_label = tk.Label(
            card, 
            text=title, 
            font=('Arial', 14, 'bold'), 
            bg=color,
            fg='white'
        )
        title_label.pack(pady=(20, 10))
        
        value_label = tk.Label(
            card, 
            text=value, 
            font=('Arial', 32, 'bold'), 
            bg=color, 
            fg='white'
        )
        value_label.pack(pady=(10, 20))
    
    def create_time_series_tab(self):
        """Create time series visualization tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📈 Time Series")
        
        self.display_image_scrollable(
            tab, 
            FIGURES_DIR / "time_series_trips.png", 
            "Daily Trip Volume Over Time"
        )
    
    def create_revenue_tab(self):
        """Create revenue visualization tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="💰 Revenue")
        
        self.display_image_scrollable(
            tab, 
            FIGURES_DIR / "revenue_analysis.png", 
            "Congestion Toll Revenue Analysis"
        )
    
    def create_zone_tab(self):
        """Create zone distribution visualization tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="📍 Zones")
        
        self.display_image_scrollable(
            tab, 
            FIGURES_DIR / "zone_category_distribution.png", 
            "Trip Distribution by Zone Category"
        )
    
    def create_leakage_tab(self):
        """Create leakage analysis visualization tab"""
        tab = ttk.Frame(self.notebook)
        self.notebook.add(tab, text="🔍 Leakage")
        
        self.display_image_scrollable(
            tab, 
            FIGURES_DIR / "leakage_analysis.png", 
            "Congestion Toll Leakage Analysis"
        )
    
    def display_image_scrollable(self, parent, image_path, title):
        """Display an image in a scrollable canvas with proper fitting"""
        # Container
        container = tk.Frame(parent, bg='white')
        container.pack(fill='both', expand=True)
        
        # Title
        title_frame = tk.Frame(container, bg='#ecf0f1', height=60)
        title_frame.pack(fill='x')
        title_frame.pack_propagate(False)
        
        title_label = tk.Label(
            title_frame,
            text=title, 
            font=('Arial', 18, 'bold'),
            bg='#ecf0f1',
            fg='#2c3e50'
        )
        title_label.pack(pady=15)
        
        try:
            if image_path.exists():
                # Load image
                img = Image.open(image_path)
                
                # Use fixed dimensions for scaling (safe defaults)
                max_width = 1200
                max_height = 650
                
                # Calculate scaling to fit while maintaining aspect ratio
                img_width, img_height = img.size
                
                # Only resize if image is larger than max dimensions
                if img_width > max_width or img_height > max_height:
                    width_ratio = max_width / img_width
                    height_ratio = max_height / img_height
                    scale_ratio = min(width_ratio, height_ratio)
                    
                    new_width = int(img_width * scale_ratio)
                    new_height = int(img_height * scale_ratio)
                    
                    # Resize image
                    img_resized = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
                else:
                    # Use original size if smaller
                    img_resized = img
                
                # Convert to PhotoImage
                photo = ImageTk.PhotoImage(img_resized)
                
                # Create canvas with scrollbars
                canvas_frame = tk.Frame(container, bg='white')
                canvas_frame.pack(fill='both', expand=True, padx=20, pady=20)
                
                canvas = Canvas(canvas_frame, bg='white', highlightthickness=0)
                
                # Scrollbars
                v_scrollbar = Scrollbar(canvas_frame, orient='vertical', command=canvas.yview)
                h_scrollbar = Scrollbar(canvas_frame, orient='horizontal', command=canvas.xview)
                
                canvas.configure(yscrollcommand=v_scrollbar.set, xscrollcommand=h_scrollbar.set)
                
                # Pack scrollbars and canvas
                v_scrollbar.pack(side='right', fill='y')
                h_scrollbar.pack(side='bottom', fill='x')
                canvas.pack(side='left', fill='both', expand=True)
                
                # Add image to canvas
                canvas.create_image(0, 0, anchor='nw', image=photo)
                canvas.image = photo  # Keep a reference
                
                # Configure scroll region
                canvas.configure(scrollregion=canvas.bbox('all'))
                
            else:
                error_label = tk.Label(
                    container, 
                    text=f"Image not found: {image_path.name}", 
                    fg='red',
                    bg='white',
                    font=('Arial', 14)
                )
                error_label.pack(pady=50)
                
        except Exception as e:
            error_label = tk.Label(
                container, 
                text=f"Error loading image: {e}", 
                fg='red',
                bg='white',
                font=('Arial', 14)
            )
            error_label.pack(pady=50)


def main():
    """
    Main entry point for the dashboard.
    """
    root = tk.Tk()
    app = NYCDashboard(root)
    root.mainloop()


if __name__ == "__main__":
    main()
