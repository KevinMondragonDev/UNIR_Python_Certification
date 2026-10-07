def main():
   lista_invitados = ["Kevin"]
   print(len(lista_invitados))
   lista_invitados.append("Carlos")
   lista_invitados.insert(1,"Ana")    
   lista_invitados.insert(0,"Pedro")
   lista_invitados.pop(2)
   print(lista_invitados)
   print(len(lista_invitados))





if __name__ == '__main__':
    main()
