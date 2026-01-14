import pandas as pd
from supabase import create_client, Client
import math
from config import SUPABASE_URL, SUPABASE_KEY

URL = SUPABASE_URL
KEY = SUPABASE_KEY
CSV_FILE = "food_ingredients_and_allergens.csv"

supabase: Client = create_client(URL, KEY)

def clean_and_upload():
    print("⏳ Membaca dataset...")
    df = pd.read_csv(CSV_FILE)

    unique_ingredients = {}

    print("⚙️ Memproses baris per baris...")
    
    for index, row in df.iterrows():
        # Ambil daftar alergi di produk ini untuk referensi
        # Contoh: "Almonds, Wheat, Dairy" -> ['Almonds', 'Wheat', 'Dairy']
        product_allergens = str(row['Allergens']).split(',')
        product_allergens = [a.strip().lower() for a in product_allergens]

        # Kumpulkan semua bahan dari kolom-kolom yang ada
        potential_ingredients = []
        
        # Kolom dataset Anda: Main Ingredient, Sweetener, Fat/Oil, Seasoning
        columns_to_check = ['Main Ingredient', 'Sweetener', 'Fat/Oil', 'Seasoning']
        
        for col in columns_to_check:
            val = str(row[col])
            # Skip jika nilainya 'None', 'nan', atau kosong
            if val.lower() in ['none', 'nan', ''] or val == 'nan':
                continue
            
            # Jika ada koma (misal: "Garlic, herbs"), pisahkan
            sub_ingredients = val.split(',')
            for item in sub_ingredients:
                clean_name = item.strip()
                if clean_name:
                    potential_ingredients.append(clean_name)

        # Proses setiap bahan yang ditemukan
        for ing_name in potential_ingredients:
            ing_lower = ing_name.lower()
            
            # LOGIKA PENCOCOKAN SEDERHANA
            # Kita mencoba menebak Allergi Group berdasarkan nama bahan
            # Jika nama bahan (misal "Almonds") ada di daftar alergi produk, kita link-kan.
            
            assigned_allergy = None
            
            # Cek apakah nama bahan persis sama dengan salah satu alergi?
            if ing_lower in product_allergens:
                assigned_allergy = ing_name.capitalize() # Misal: Almonds -> Almonds
            
            # Cek logika manual untuk kasus umum (Hardcoded logic untuk akurasi)
            elif "butter" in ing_lower or "cheese" in ing_lower or "milk" in ing_lower:
                assigned_allergy = "Dairy"
            elif "flour" in ing_lower or "bread" in ing_lower:
                assigned_allergy = "Wheat"
            elif "shrimp" in ing_lower or "crab" in ing_lower:
                assigned_allergy = "Seafood"
            
            # Simpan ke dictionary unik
            # Kita pakai 'upsert' logic di dictionary: jika sudah ada dan punya allergy, jangan ditimpa null
            if ing_lower not in unique_ingredients:
                unique_ingredients[ing_lower] = {
                    'name': ing_name,
                    'allergy_group': assigned_allergy,
                    'risk_level': 'High' if assigned_allergy else 'Medium'
                }
            else:
                # Update jika sebelumnya null tapi sekarang ketemu mapping-nya
                if unique_ingredients[ing_lower]['allergy_group'] is None and assigned_allergy is not None:
                    unique_ingredients[ing_lower]['allergy_group'] = assigned_allergy

    # --- UPLOAD KE SUPABASE ---
    print(f"🚀 Mulai upload {len(unique_ingredients)} bahan unik ke Supabase...")
    
    data_list = list(unique_ingredients.values())
    
    # Upload per 500 data (Batching)
    batch_size = 500
    for i in range(0, len(data_list), batch_size):
        batch = data_list[i:i+batch_size]
        try:
            # Gunakan upsert=True agar jika data sudah ada, tidak error (diupdate)
            response = supabase.table('ingredients').upsert(batch, on_conflict='name').execute()
            print(f"✅ Batch {i} - {i+len(batch)} terupload.")
        except Exception as e:
            print(f"❌ Error di batch {i}: {e}")

    print("🎉 Selesai! Data bahan makanan siap digunakan.")

if __name__ == "__main__":
    clean_and_upload()